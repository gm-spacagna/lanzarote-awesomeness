package com.barclays.adacore

import com.barclays.adacore.utils.{Logger, TopElements}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scalaz.Scalaz._

case class CharisRecommender(@transient sc: SparkContext, minNumTransactions: Int) extends RecommenderTrainer {
  def train(data: RDD[AnonymizedRecord]): Recommender = {

    val activeUsers: Set[Long] =
      data.map(_.maskedCustomerId -> 1).reduceByKey(_ + _)
      .filter(_._2 > 5).collect().map(_._1).toSet
    val activeUsersBV = sc.broadcast(activeUsers)

    val activeBusinesses: Set[(String, String)] =
      data.map(_.businessKey -> 1)
      .reduceByKey(_ + _)
      .filter(_._2 > 20)
      .collect().map(_._1).toSet
    val activeBusinessesBV = sc.broadcast(activeBusinesses)

    val filteredUserAmountBusiness = data.filter(transaction =>
      activeUsersBV.value(transaction.maskedCustomerId) && activeBusinessesBV.value(transaction.businessKey)
    )


    val customerToBusinessStatistics = filteredUserAmountBusiness.groupBy(line => (line.maskedCustomerId, line.businessKey)).map(grouped => grouped._2).
                                       map(records => {
                                         val spends = records.groupBy(row => row.maskedCustomerId).map(groups => groups._2.map(trans => trans.amount))
                                         val sumSpends = spends.map(x => x.sum).head
                                         val visits = spends.map(x => x.size).head
                                         val firstElement = records.head

                                         CustomerToBusinessStatistics(firstElement.maskedCustomerId,
                                           firstElement.generalizedCategoricalGroup,
                                           sumSpends,
                                           visits,
                                           firstElement.merchantCategoryCode,
                                           firstElement.businessName,
                                           firstElement.businessTown,
                                           firstElement.businessPostcode)

                                       })

    val uniqueVisitorsPerBusiness =
      customerToBusinessStatistics.map(row => (row.businessKey, 1)).reduceByKey(_ + _)

    // val mappedCompanies = cleaned.keyBy(_.businessKey).groupByKey()

    val totalAmountPerBusiness =
      customerToBusinessStatistics.map(row => (row.businessKey, row.amountSum)).reduceByKey(_ + _)


    val totalVisitsPerBusiness =
      customerToBusinessStatistics.map(row => (row.businessKey, row.visits)).reduceByKey(_ + _)



    val totalAmountPerCustomer =
      customerToBusinessStatistics.map(row => (row.maskedCustomerId, row.amountSum)).reduceByKey(_ + _)


    val totalVisitsPerCustomer =
      customerToBusinessStatistics.map(row => (row.maskedCustomerId, row.visits)).reduceByKey(_ + _)


    val totalBusinessesOfCustomer =
      customerToBusinessStatistics.map(row => (row.maskedCustomerId, 1)).reduceByKey(_ + _)

    val ratingsWithSize = customerToBusinessStatistics
                          .groupBy(tup => tup.businessKey)
                          .join(uniqueVisitorsPerBusiness)
                          .join(totalAmountPerBusiness)
                          .join(totalVisitsPerBusiness)
                          .flatMap(joined => {
                            joined._2._1._1._1.map(f => (f.maskedCustomerId, f.businessKey, f.visits, f.amountSum, f.merchantCategoryCode,
                              joined._2._1._1._2, joined._2._1._2, joined._2._2))
                          })
                          .groupBy(tup => tup._1)
                          .join(totalAmountPerCustomer)
                          .join(totalVisitsPerCustomer)
                          .join(totalBusinessesOfCustomer)
                          .flatMap(joined => {
                            joined._2._1._1._1.map(f => (f._1, f._2, f._3, f._4, f._5, f._6, f._7, f._8, joined._2._1._1._2, joined._2._1._2, joined._2._2))
                          }).map(x => CustomerToBusinessRating(x._1,
                            x._2,
                            x._3,
                            x._4,
                            x._5,
                            x._6,
                            x._7,
                            x._8,
                            x._9,
                            x._10,
                            x._11))


    val ratingPairs =
      ratingsWithSize
      .keyBy(tup => tup.maskedCustomerId)
      .join(ratingsWithSize.keyBy(tup => tup.maskedCustomerId))
      .filter(entry => entry._2._1.businessKey.productIterator.toList.mkString("--") <
        entry._2._2.businessKey.productIterator.toList.mkString("--"))

    val vectorCalcs =
      ratingPairs
      .map(data => {
        val key = (data._2._1.businessKey, data._2._2.businessKey)
        val stats =
          (data._2._1.numRatersPerBusiness, // number of raters for buz 1
            data._2._2.numRatersPerBusiness) // number of raters for buz 2
        (key, stats)
      })
      .groupByKey()
      .map(data => {
        val key = data._1
        val vals = data._2
        val size = vals.size
        val numRaters = vals.map(f => f._1).max
        val numRaters2 = vals.map(f => f._2).max
        (key, (size, numRaters, numRaters2))
      })


    val similarities =
      vectorCalcs
      .map(fields => {

        val key = fields._1
        val (size, numRaters, numRaters2) = fields._2

        val jaccard = utils.Similarities.jaccardSimilarity(size, numRaters, numRaters2)

        (key, jaccard)
      })

    val scores = similarities.map({ case ((b1, b2), score) => (b1, (b2, score))

    }).union(similarities.map({ case ((b1, b2), score) => (b2, (b1, score))

    }))

    val topNBusinesses = scores.groupByKey.mapValues(scores => scores.toList.sortBy(_._2).reverse.take(20)).collectAsMap

    val customerToBusinesses = ratingsWithSize.map(record => record.maskedCustomerId -> record.businessKey).groupByKey().take(10)
    val customerToRecommendations = customerToBusinesses.map(visitedBusinesses => (
      visitedBusinesses._1,
      visitedBusinesses._2.toSet,
      visitedBusinesses._2.flatMap(x => topNBusinesses(x))
      .groupBy(_._1).map(
        x => (x._1, x._2.map(_._2).sum)).toList.filter(
        el => !visitedBusinesses._2.toSet.exists(_ == el._1)).sortBy(_._2).reverse.map(_._1).take(20)

      )).map(entry => (entry._1, entry._3))





    ////
    ///

    val filteredCustomersAndBusinesses: RDD[(Long, (String, String))] =
      data.map(record => (record.maskedCustomerId, record.businessKey) -> 1).reduceByKey(_ + _)
      .filter(_._2 >= minNumTransactions).keys.cache()

    val customerIdToBusinessSet: Broadcast[Map[Long, Set[(String, String)]]] =
      sc.broadcast(filteredCustomersAndBusinesses.mapValues(Set(_)).reduceByKey(_ ++ _).collect().toMap)

    val businessKeyToCustomerSet: RDD[((String, String), Set[Long])] =
      filteredCustomersAndBusinesses.map(_.swap).mapValues(Set(_)).reduceByKey(_ ++ _).cache()

    val item2itemMatrix: Broadcast[Map[(String, String), List[((String, String), Double)]]] = sc.broadcast(
      businessKeyToCustomerSet.cartesian(businessKeyToCustomerSet).flatMap {
        case ((businessKey1, userSet1), (businessKey2, userSet2)) =>
          val conditionalProb =
            userSet1.intersect(userSet2).size.toDouble /
              (userSet1.size + userSet2.size - userSet1.intersect(userSet2).size.toDouble)
          (businessKey1 != businessKey2 && conditionalProb > 0)
          .option(businessKey1 -> List(businessKey2 -> conditionalProb))
      }
      .reduceByKey(_ ++ _)
      .collect().toMap
    )

    val rankedBusinessesByNumberOfCustomers = sc.broadcast(
      businessKeyToCustomerSet.mapValues(_.size.toDouble).sortBy(_._2, ascending = false).collect().toList
    )

    filteredCustomersAndBusinesses.unpersist()
    businessKeyToCustomerSet.unpersist()

    new Recommender {
      // returns customerId -> List[(merchantName, merchantTown)]
      def recommendations(customers: RDD[Long], n: Int): RDD[(Long, List[(String, String)])] =
        customers.map(customerId =>
          customerId ->
            ((customerIdToBusinessSet.value.get(customerId) match {
              case Some(customerBusinessKeys) =>
                (for {
                  customerBusinessKey <- customerBusinessKeys
                  similarityRow <- item2itemMatrix.value.get(customerBusinessKey).toList
                  (similarBusiness, conditionalProb) <- similarityRow
                  if !customerBusinessKeys.contains(similarBusiness)
                } yield similarBusiness -> conditionalProb)
                .groupBy(_._1).mapValues(_.map(_._2).sum).toList
              case None => rankedBusinessesByNumberOfCustomers.value.take(n)
            }) |> { businessesScores =>
              val recommendations = TopElements.topN(businessesScores)(_._2, n).map(_._1)
              if (recommendations.size >= n) recommendations
              else (recommendations ++
                (rankedBusinessesByNumberOfCustomers.value.take(n).map(_._1).toSet -- recommendations)).take(n)
            })
        )
    }
  }
}
