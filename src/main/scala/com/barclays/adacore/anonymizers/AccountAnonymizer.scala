package com.barclays.adacore.anonymizers

import com.barclays.adacore.RawAccount
import com.barclays.adacore.utils.VPTree
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._
import scala.util.Random

case class LongRange(start: Long, end: Long)

case class CategoricalBucket(onlineActive: Boolean, acornTypeId: Int,
                             gender: String, maritalStatusId: Option[Int], occupationId: Option[Int])

case class NumericalBucket(balance: Long, age: Int, income: Long, grossIncome: Long)

case class GeneralizedCategoricalBucketGroup(group: Set[CategoricalBucket], groupSize: Int)

case class GeneralizedNumericalBucketGroup(balanceRange: LongRange, ageRange: LongRange,
                                           incomeRange: LongRange, grossIncomeRange: LongRange, groupSize: Int)

case class MaskedAndGeneralizedRawAccount(maskedCustomerId: Long, generalizedRawAccount: GeneralizedCategoricalBucketGroup)

object AccountAnonymizer {

  implicit class PimpedRawAccount(rawAccount: RawAccount) {
    def categoricalBucket: CategoricalBucket = rawAccount match {
      case RawAccount(_, _, _, onlineActive, _, _, _, acornTypeId, gender, maritalStatusId, occupationId) =>
        CategoricalBucket(onlineActive, acornTypeId, gender, maritalStatusId, occupationId)
    }
  }

  def generalizeByCategoricalBuckets(@transient sc: SparkContext, rawAccounts: RDD[RawAccount], k: Int = 10): RDD[(GeneralizedCategoricalBucketGroup, Iterable[RawAccount])] = {
    val reducedMapsRDD =
      rawAccounts.map(_.categoricalBucket -> 1).reduceByKey(_ + _).cache()

    val bigBuckets: Array[(CategoricalBucket, Int)] = reducedMapsRDD.filter(_._2 >= k).collect()

    val smallBuckets: Array[(CategoricalBucket, Int)] = reducedMapsRDD.filter(_._2 < k).collect()

    val vpTree = VPTree(items = smallBuckets, distance = bucketsDistance, 1)

    val mergedGroups: Map[Set[CategoricalBucket], Int] = mergeBuckets(smallBuckets.toMap, vpTree, k).toMap

    val replacementGroups: Map[Set[CategoricalBucket], (Set[CategoricalBucket], Int)] =
      mergedGroups.flatMap {
        case smallGroup@(smallGroupBuckets, smallGroupCount) if smallGroupCount < k && mergedGroups.size > 1 =>
          val (mostSimilarBuckets, mostSimilarCount) = (mergedGroups - smallGroupBuckets).minBy {
            case (neighbourBuckets, neighbourCount) =>
              (for {
                bucket <- smallGroupBuckets
                neighbourBucket <- neighbourBuckets
              } yield bucketsDistance(bucket, neighbourBucket)
                ).sum / neighbourBuckets.size
          }
          List(mostSimilarBuckets ->(mostSimilarBuckets ++ smallGroupBuckets, mostSimilarCount + smallGroupCount),
            smallGroupBuckets ->(Set.empty[CategoricalBucket], 0))
        case smallGroup@(smallGroupBuckets, smallGroupCount) if smallGroupCount < k =>
          throw new IllegalArgumentException("Impossible to generalize the current dataset with k = " + k)
        case _ => Nil
      }

    val bucketToGroup =
      mergedGroups
        .flatMap {
          case group@(buckets, _) if replacementGroups.contains(buckets) =>
            val replacement@(replacementGroup, _) = replacementGroups(buckets)
            replacementGroup.map(bucket => bucket -> replacement)
          case group@(buckets, _) => buckets.map(bucket => bucket -> group)
        } ++ bigBuckets.map {
        case (bucket, count) => bucket ->(Set(bucket), count)
      }

    assert(bucketToGroup.values.forall(_._2 >= k),
      "Some bucket group has less than " + k + " elements, something fishy going on."
    )

    val bucketToGroupBV: Broadcast[Map[CategoricalBucket, (Set[CategoricalBucket], Int)]] =
      sc.broadcast(bucketToGroup)

    rawAccounts.groupBy(rawAccount => {
      val (group, count) = bucketToGroupBV.value(rawAccount.categoricalBucket)
      GeneralizedCategoricalBucketGroup(group, count)
    })
  }

  val bucketsDistance: (CategoricalBucket, CategoricalBucket) => Double = {
    case ((cat1: CategoricalBucket), (cat2: CategoricalBucket)) =>
      cat1.productIterator.zip(cat2.productIterator).count(x => x._1 != x._2)
  }

  def mergeBuckets(smallBuckets: Map[CategoricalBucket, Int], vpTree: VPTree[CategoricalBucket, Int],
                   k: Int, prevBuckets: Set[CategoricalBucket] = Set.empty): List[(Set[CategoricalBucket], Int)] = {
    if (smallBuckets.isEmpty) Nil
    else {
      val bucket = smallBuckets.keys.head // take a random one
      (1 to bucket.productArity).foldLeft((Set.empty[CategoricalBucket], 0)) {
        case (prev@(prevGroup, prevSize), maxDist) => {
          if (prevSize < k) {
            // TODO: Remove elements from VPTree at each iteration
            val neighbors = vpTree.nearest(bucket, maxDist).toMap.--(prevBuckets).toList.sortBy {
              case (neighborBucket, _) => (bucketsDistance(neighborBucket, bucket), neighborBucket.toString)
            }
            //println("nearest items for bucket " + bucket + " and maxDist = " + maxDist + " are " + neighbors)

            neighbors.foldLeft((Set.empty[CategoricalBucket], 0)) {
              // refactor to use recursion to return early
              case (acc@(accNeighbours, accCount), (neighbourBucket, neighbourCount)) =>
                if (accCount < k) (accNeighbours + neighbourBucket, accCount + neighbourCount)
                else acc
            }
          }
          else prev
        }
      } match {
        case group@(buckets, size) =>
          group +: mergeBuckets(smallBuckets -- buckets, vpTree, k, prevBuckets ++ buckets)
      }
    }
  }

  val distance: (GeneralizedNumericalBucketGroup, GeneralizedNumericalBucketGroup) => Double = {
    case (x, y) => {
      val x1 = Math.min(Math.abs(x.ageRange.start - y.ageRange.end), Math abs (x.ageRange.end - y.ageRange.start))
      val x2 = Math.min(Math.abs(x.balanceRange.start - y.balanceRange.end), Math abs (x.balanceRange.end - y.balanceRange.start))
      val x3 = Math.min(Math.abs(x.grossIncomeRange.start - y.grossIncomeRange.end), Math abs (x.grossIncomeRange.end - y.grossIncomeRange.start))
      val x4 = Math.min(Math.abs(x.incomeRange.start - y.incomeRange.end), Math abs (x.incomeRange.end - y.incomeRange.start))
      Math.sqrt(x1 * x1 + x2 * x2 + x3 * x3 + x4 * x4)
    }
  }

  def generalizeByNumericalBuckets(numericalBuckets: Iterable[NumericalBucket], k: Int = 10): Set[GeneralizedNumericalBucketGroup] = {
    val groups = numericalBuckets.map(x => GeneralizedNumericalBucketGroup(LongRange(x.balance, x.balance),
      LongRange(x.age, x.age), LongRange(x.income, x.income),
      LongRange(x.grossIncome, x.grossIncome), 1))
      .groupBy(identity).mapValues(_.size).map(pair => pair._1.copy(groupSize = pair._2)).toSet
    mergeNumBucket(groups, k)
  }

  def mergeNumBucket(generalizedBuckets: Set[GeneralizedNumericalBucketGroup],
                     k: Int = 10): Set[GeneralizedNumericalBucketGroup] = {
    val partitions =
      List(splitByAge(generalizedBuckets), splitByBalance(generalizedBuckets),
      splitByGrossIncome(generalizedBuckets), splitByIncome(generalizedBuckets))
    val partition = partitions.maxBy(part => balancedNess(part._1, part._2))

    if (partition._1.size < k || partition._2.size < k)
      return Set((partition._1 ++ partition._2).reduce((b1, b2) => mergeTwoGeneralizedNumericalBuckets(b1, b2)))
    else {
      return mergeNumBucket(partition._1) ++ mergeNumBucket(partition._2)
    }
  }

  def balancedNess(p1: Set[GeneralizedNumericalBucketGroup], p2: Set[GeneralizedNumericalBucketGroup]): Int = {
    val ageSetSize1 = p1.map(_.ageRange).size
    val balanceSetSize1 = p1.map(_.balanceRange).size
    val incomeSetSize1 = p1.map(_.incomeRange).size
    val grossIncomeSetSize1 = p1.map(_.grossIncomeRange).size

    val ageSetSize2 = p2.map(_.ageRange).size
    val balanceSetSize2 = p2.map(_.balanceRange).size
    val incomeSetSize2 = p2.map(_.incomeRange).size
    val grossIncomeSetSize2 = p2.map(_.grossIncomeRange).size

    Math.abs(ageSetSize1 - ageSetSize2) + Math.abs(balanceSetSize1 - balanceSetSize2) +
      Math.abs(incomeSetSize1 - incomeSetSize2) + Math.abs(grossIncomeSetSize1 - grossIncomeSetSize2)
  }

  def splitByAge(buckets: Set[GeneralizedNumericalBucketGroup]) : (Set[GeneralizedNumericalBucketGroup], Set[GeneralizedNumericalBucketGroup]) = {
    val medianRange: LongRange = rangeMedian(buckets.map(_.ageRange).toSeq)
    buckets.partition(b => if (b.ageRange.end < medianRange.start) true else if(b.ageRange.end == medianRange.start) Random.nextBoolean() else false)
  }

  def splitByBalance(buckets: Set[GeneralizedNumericalBucketGroup]) : (Set[GeneralizedNumericalBucketGroup], Set[GeneralizedNumericalBucketGroup]) = {
    val medianRange = rangeMedian(buckets.map(_.balanceRange).toSeq)
    buckets.partition(b => if (b.balanceRange.end < medianRange.start) true else if(b.balanceRange.end == medianRange.start) Random.nextBoolean() else false)
  }

  def splitByIncome(buckets: Set[GeneralizedNumericalBucketGroup]) : (Set[GeneralizedNumericalBucketGroup], Set[GeneralizedNumericalBucketGroup]) = {
    val medianRange = rangeMedian(buckets.map(_.incomeRange).toSeq)
    buckets.partition(b => if (b.incomeRange.end < medianRange.start) true else if(b.incomeRange.end == medianRange.start) Random.nextBoolean() else false)
  }

  def splitByGrossIncome(buckets: Set[GeneralizedNumericalBucketGroup]) : (Set[GeneralizedNumericalBucketGroup], Set[GeneralizedNumericalBucketGroup]) = {
    val medianRange = rangeMedian(buckets.map(_.grossIncomeRange).toSeq)
    buckets.partition(b => if (b.balanceRange.end < medianRange.start) true else if(b.balanceRange.end == medianRange.start) Random.nextBoolean() else false)
  }

  def mergeTwoGeneralizedNumericalBuckets(g1: GeneralizedNumericalBucketGroup, g2: GeneralizedNumericalBucketGroup): GeneralizedNumericalBucketGroup = {
    val minAge = Math.min(g1.ageRange.start, g2.ageRange.start)
    val maxAge = Math.max(g1.ageRange.end, g2.ageRange.end)
    val minBalance = Math.min(g1.balanceRange.start, g2.balanceRange.start)
    val maxBalance = Math.max(g1.balanceRange.end, g2.balanceRange.end)
    val minIncome = Math.min(g1.incomeRange.start, g2.incomeRange.start)
    val maxIncome = Math.max(g1.incomeRange.end, g2.incomeRange.end)
    val minGrossIncome = Math.min(g1.grossIncomeRange.start, g2.grossIncomeRange.start)
    val maxGrossIncome = Math.max(g1.grossIncomeRange.end, g2.grossIncomeRange.end)

    GeneralizedNumericalBucketGroup(LongRange(minBalance, maxBalance), LongRange(minAge, maxAge), LongRange(minIncome, maxIncome),
      LongRange(minGrossIncome, maxGrossIncome), g1.groupSize + g2.groupSize)
  }


  def rangeMedian(s: Seq[LongRange]): LongRange = {
    val (lower, upper) = s.sortBy(_.start).splitAt(s.size / 2)
    if (s.size % 2 == 0) LongRange((lower.last.start + upper.head.start) / 2, (lower.last.end + upper.head.end) / 2) else upper.head
  }

  def median(s: Seq[Double]) = {
    val (lower, upper) = s.sorted.splitAt(s.size / 2)
    if (s.size % 2 == 0) (lower.last + upper.head) / 2.0 else upper.head
  }

  def binIntoPercentiles(values: Array[Double], bins: Int): Array[Double] = {
    val rangeLength = values.length.toDouble / bins.toDouble

    (for {i <- 0 to bins - 1} yield values.sorted.apply(Math.floor(i * rangeLength).toInt)).toArray
  }
}
