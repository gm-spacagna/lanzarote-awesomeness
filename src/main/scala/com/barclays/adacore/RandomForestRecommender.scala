package com.barclays.adacore

import breeze.linalg.DenseVector
import com.barclays.adacore.jobs.Stats
import com.barclays.adacore.utils.TopElements
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{Vectors, Vector, SparseVector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD

import scalaz.Scalaz._

case class RandomForestRecommender(@transient sc: SparkContext, minNumTransactions: Int) extends RecommenderTrainer {
  def businessID(tx: AnonymizedRecord) = (tx.businessName, tx.businessTown)

  val merchantCodes =
    List("5051", "5044", "5462", "7523", "4784", "5814", "5441", "5994", "5122", "5451", "7922",
      "8651", "5973", "7832", "7395", "5499", "5912", "5947", "4112", "5192", "5812", "5251", "7933", "8699", "7929",
      "5942", "8398", "7251", "5921", "7997", "7991", "5813", "5131", "5943", "5735", "5937", "7841", "7216", "7210",
      "5111", "5411", "7999", "5970", "5995", "5422", "5993", "8661", "5310", "5169", "7932", "7393", "4814", "5399",
      "5983", "5722", "5811", "7998", "7996", "5541", "5331", "5651", "3741", "5714", "5699", "5261", "5621", "5977",
      "5099", "4812", "5542", "7941", "7993", "5691", "7911", "4011", "7221", "5992", "7296", "7032", "7211", "4582",
      "0763", "5193", "0780", "5999", "7230", "5311", "7992", "5039", "5998", "5945", "5641", "8049", "9399", "7333",
      "5137", "2791", "5949", "7338", "8641", "7342", "5940", "5697", "7298", "2741", "5211", "5065", "5655", "8041",
      "5661", "8031", "4215", "5139", "3514", "5950", "7542", "7297", "5309", "4131", "8211", "5200", "3604", "7829",
      "5941", "5198", "3246", "7995", "7994", "8071", "5631", "7011", "6011", "7629", "3405", "5611", "7379", "7399",
      "5719", "5231", "4899", "5971", "5948", "5072", "8249", "5931", "5046", "2842", "8220", "3501", "4121", "0742",
      "5935", "5996", "5045", "3615", "7394", "3672", "7372", "7622", "4900", "7692", "7277", "5533", "3692", "5300",
      "3502", "8021", "5094", "5733", "5085", "5199", "7033", "8911", "8299", "7311", "5698", "7361", "4457", "3509",
      "9402", "5734", "3681", "4411", "8050", "3637", "8351", "3750", "4816", "7631", "5932", "3811", "4214", "5944",
      "7392", "1731", "7349", "3381", "5976", "7534", "7299", "5946", "5532", "8043", "8099", "7623", "5732", "4225",
      "1771", "7339", "8011", "3504", "8999", "7641", "3503", "3755", "3351", "1520", "5047", "4468", "5074", "1761",
      "4111", "8062", "9222", "8042", "7699", "4789", "7512", "7549", "1799", "1711", "7535", "3779", "8244", "5013",
      "5933", "5713", "3015", "4511", "5551", "3395", "7513", "5021", "7531", "1750", "6300", "5712", "3008", "5571",
      "3051", "8931", "5972", "5718", "6051", "7217", "6010", "8111", "7276", "7538", "1740", "5561", "6513", "5975",
      "5599", "5271", "4829", "4722", "5592", "7261", "5511", "5521", "6012", "7519", "6211", "3441")
    .zipWithIndex.map(e => e._1 -> e._2.toDouble).toMap

  val occupationalID =
    List("1,9", "9,12", "1,10,12", "5,7,13", "1,5,10,11", "6,7", "1,4", "1,4,5,13", "5,9,10,12,13",
      "10,13,14", "1,7", "3,5,13", "9,14,15", "12,15", "1,12,15", "1,8,13", "3,13", "7,12", "12", "1,9,14", "9,12,15", "1,9,13",
      "1,13", "14", "5,6,9,11", "4", "1,12", "3,4,5,6", "8,12", "1,6", "8,14", "3,5,11", "1,2,7,9,11", "1,2,13,14", "4,11,13", "4,6",
      "2,5", "4,5", "5,7,15", "10", "6,11,12", "13", "2,3,15", "1,5,6", "3,7", "1,11", "1,2,13", "1,5,14", "", "3,6,7", "1,3,12",
      "1,5,13", "2,7", "3,12", "4,6,11", "1,4,6,14", "6", "1,3", "14,15", "2,15", "4,9,13", "7,9", "6,9", "3,10,11", "2,10", "10,15",
      "1,7,12,13", "3", "9,11", "4,12,15", "3,7,12,14", "7,12,13", "2", "1,10", "3,11", "1,8", "1,7,15", "7,8", "9,15", "2,8", "7,10",
      "2,4", "2,7,14,15", "5", "13,15", "4,8,10", "7,9,10,12", "7,8,9", "3,7,10", "6,11", "2,12,14", "1,10,14,15", "3,4,6,7,10",
      "10,11,13", "1,2", "2,6,7,8,9", "13,14", "7", "5,11", "1,4,15", "11", "6,10,11", "8", "12,13,14", "4,7", "3,14", "6,7,8,10,11",
      "4,7,13", "12,14", "3,7,8,11", "5,7,11", "1,7,8", "8,10", "2,4,7", "3,5,7,13", "8,13", "1,4,6,8,10,12", "9,11,15", "8,9,13,14",
      "4,6,8,14", "1,3,4,14", "9,13", "5,11,13", "1,4,7,14", "15", "6,10,11,13", "6,10", "5,6,13", "3,10", "4,13,14", "12,13", "11,15",
      "10,14", "6,12", "2,9", "1,4,8,14", "5,8", "6,9,12,13", "3,9", "3,13,14,15", "2,7,14", "2,5,15", "1,3,5,12,13", "1,2,6,12",
      "12,14,15", "1,3,6,7", "2,3,13", "6,14", "9,10,12", "4,12", "1", "2,7,9,12,15", "5,12", "5,14", "2,5,8", "4,7,14", "1,4,13",
      "6,13", "5,9,14,15", "5,7,9", "3,6", "1,11,12", "2,12", "1,10,13,14", "4,5,6", "1,4,9,13", "8,10,11", "2,7,13", "5,10,11",
      "3,10,13,14", "1,14", "2,4,13", "1,2,15", "8,10,12,13", "4,5,9", "1,12,13", "10,11,14", "7,9,13", "7,15", "3,4,14",
      "1,4,12,15", "2,14,15", "2,4,9,14,15", "6,12,13", "4,9", "8,10,11,13", "5,12,14", "10,11", "2,11", "4,6,7,10,13,14",
      "1,5,14,15", "2,13", "4,10", "3,4,9", "1,8,12", "1,5,7,9,14", "9", "2,13,14", "2,14", "9,14", "7,9,14", "2,4,12,14",
      "1,3,12,13", "2,9,13", "4,8", "4,12,14", "2,6", "2,8,13", "1,4,5,8", "1,3,7", "9,11,13", "3,7,11", "3,9,12,14,15",
      "1,2,12", "6,7,10", "5,10", "1,5", "3,5,9", "1,9,10", "6,10,11,15", "3,5", "1,4,5,8,13", "6,8", "5,6", "6,8,15", "11,13",
      "1,15", "11,12,14", "1,5,12", "1,8,12,13", "3,4", "5,7", "4,11", "5,13")
    .zipWithIndex.map(e => e._1 -> e._2.toDouble).toMap

  val genderIndex = List("M", "F", " ").zipWithIndex.map(e => e._1 -> e._2.toDouble).toMap

  val maritalStatusIndex =
    List("2,4,5,6", "1,3,5", "0,1,4", "0,6", "2,3,4,6", "1,3,4,5", "0,4,6", "0,3", "2,4", "2,3,4,5",
      "2,3,5", "0,2,4", "1,2,4", "0,4", "3,4,6", "4,5,6", "0,5,6", "1,4,6", "1,3,6", "2,4,6", "1,2,3", "2,3,6", "5,6", "1,3", "1,3,4",
      "3,5,6", "2,5", "1,2,6", "2,5,6", "2,3", "4,6", "3,4,5", "1,4", "1,5", "1,2", "1,2,5", "3,5", "4,5", "1,6", "2,6", "3,6", "3,4", "4",
      "", "5", "3", "6", "1", "2")
    .zipWithIndex.map(e => e._1 -> e._2.toDouble).toMap

  def fromGenderToIndex(u: AnonymizedRecord): Double = u.gender match {
    case s if s.nonEmpty => genderIndex(s.head.trim)
    case _ => genderIndex(" ")
  }

  def fromOnline(u: AnonymizedRecord): Double = u.onlineActive match {
    case s if s.nonEmpty => s.foldLeft(false)((l, r) => l | r) ? 1.0 | 2.0
    case _ => 0.0
  }

  def fromOccupational(u: AnonymizedRecord) = occupationalID(u.occupationId.flatten.toList.sorted.mkString(","))

  def fromMerchantCodes(u: AnonymizedRecord) = merchantCodes(u.merchantCategoryCode)

  def dayOfWeek(u: AnonymizedRecord) = u.dayOfWeek.toDouble

  def fromMarital(u: AnonymizedRecord) = maritalStatusIndex(u.maritalStatusId.flatten.toList.sorted.mkString(","))

  val featFun: Array[(AnonymizedRecord) => Double] = Array(fromMerchantCodes _, fromGenderToIndex _, dayOfWeek _,
    fromMarital _, fromOccupational _)

  def fromUserToFeatures(u: AnonymizedRecord) = new DenseVector(featFun.map(f => f(u)))

  def features(records: RDD[AnonymizedRecord]): RDD[(Long, Vector)] = {

    val custFeatures: RDD[DenseVector[Double]] = records.map(r => r.maskedCustomerId -> r)
                                                 .reduceByKey((l, r) => l)
                                                 .map(u => fromUserToFeatures(u._2))

    val entries: RDD[((Long, (String, String)), (Int, Double))] =
      records.keyBy(r => (r.maskedCustomerId, businessID(r)))
      .mapValues(value => (1, value.amount))
      .reduceByKey(_ |+| _)

    val businessIdxMap = entries.map(_._1._2).distinct.zipWithIndex.mapValues(_.toInt).collect().toMap

    val businessIdx: Broadcast[Map[(String, String), Int]] =
      entries.context.broadcast(entries.map(_._1._2).distinct.zipWithIndex.mapValues(_.toInt).collect().toMap)

    //    val custIdx: Broadcast[Map[Long, Long]] = records.context.broadcast(entries.map(_._1._1).distinct().zipWithIndex().collect().toMap)
    //
    //    val history: RDD[(Long, List[(String, String)])] =
    //      entries.map(e => e._1._1 -> List((e._1._2, e._2)))
    //      .reduceByKey(_ |+| _)
    //      .mapValues(v => v.sortBy(el => -el._2._2).map(_._1))

    entries.keyBy(_._1._1).mapValues(v => List((v._1._2, v._2)))
    .reduceByKey(_ |+| _)
    .mapPartitions(part => {
      val busIdxBV = businessIdx.value

      part.map(e => {
        val (cId, col) = e
        val sv = Vectors.sparse(busIdxBV.size, col.map(el => busIdxBV(el._1) -> el._2._1.toDouble))
        (cId, sv)
      })
    })
  }

  def train(data: RDD[AnonymizedRecord]): Recommender = {

    val trainingData = features(data)

    val entries: RDD[((Long, (String, String)), (Int, Double))] =
      data.keyBy(r => (r.maskedCustomerId, businessID(r)))
      .mapValues(value => (1, value.amount))
      .reduceByKey(_ |+| _)

    val businessIdx: Map[(String, String), Int] = entries.map(_._1._2).distinct.zipWithIndex.mapValues(_.toInt).collect().toMap

    val models =
      for {
        (business, id) <- businessIdx
        labels = trainingData.map(v => LabeledPoint(Math.max(v._2.apply(id), 1d), v._2))
        model = RandomForest.trainClassifier(labels, 2, Map[Int, Int](), 40, "auto", "variance", 4, 32)
      } yield (business, model)

    val filteredCustomersAndBusinesses: RDD[(Long, (String, String))] =
      data.map(record => (record.maskedCustomerId, record.businessKey) -> 1).reduceByKey(_ + _)
      .filter(_._2 >= minNumTransactions).keys.cache()

    val customerIdToBusinessSet: Broadcast[Map[Long, Set[(String, String)]]] =
      sc.broadcast(filteredCustomersAndBusinesses.mapValues(Set(_)).reduceByKey(_ ++ _).collect().toMap)

    val businessKeyToCustomerSet: RDD[((String, String), Set[Long])] =
      filteredCustomersAndBusinesses.map(_.swap).mapValues(Set(_)).reduceByKey(_ ++ _).cache()

    val rankedBusinessesByNumberOfCustomers = sc.broadcast(
      businessKeyToCustomerSet.mapValues(_.size.toDouble).sortBy(_._2, ascending = false).collect().toList
    )

    //    new Recommender {
    //      // returns customerId -> List[(merchantName, merchantTown)]
    //      def recommendations(customers: RDD[Long], n: Int): RDD[(Long, List[(String, String)])] =
    //        customers.map(customerId =>
    //          customerId ->
    //            ((customerIdToBusinessSet.value.get(customerId) match {
    //              case Some(customerBusinessKeys) =>
    //                (for {
    //                  model <- models
    //                  model._2.predict()
    //                  similarityRow <- item2itemMatrix.value.get(customerBusinessKey).toList
    //                  (similarBusiness, conditionalProb) <- similarityRow
    //                  if !customerBusinessKeys.contains(similarBusiness)
    //                } yield similarBusiness -> conditionalProb)
    //                .groupBy(_._1).mapValues(_.map(_._2).sum).toList
    //              case None => rankedBusinessesByNumberOfCustomers.value.take(n)
    //            }) |> { businessesScores =>
    //              val recommendations = TopElements.topN(businessesScores)(_._2, n).map(_._1)
    //              if (recommendations.size >= n) recommendations
    //              else (recommendations ++
    //                (rankedBusinessesByNumberOfCustomers.value.take(n).map(_._1).toSet -- recommendations)).take(n)
    //            })
    //        )
    //    }
    ???
  }
}
