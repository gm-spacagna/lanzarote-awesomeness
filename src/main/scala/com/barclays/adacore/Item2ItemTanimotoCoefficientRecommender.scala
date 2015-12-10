package com.barclays.adacore

import com.barclays.adacore.utils.TopElements
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scalaz.Scalaz._

case class Item2ItemTanimotoCoefficientRecommender(@transient sc: SparkContext, minNumTransactions: Int) extends RecommenderTrainer {
  def train(data: RDD[AnonymizedRecord]): Recommender = {

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
