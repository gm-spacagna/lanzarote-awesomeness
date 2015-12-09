package com.barclays.adacore

import com.barclays.adacore.utils.TopElements
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import scalaz.Scalaz._

case class Item2ItemConditionalProbabilityRecommender(@transient sc: SparkContext, minNumTransactions: Int) extends RecommenderTrainer {
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
          val conditionalProb = userSet1.intersect(userSet2).size.toDouble / userSet2.size

          (businessKey1 != businessKey2 && conditionalProb > 0)
          .option(businessKey1 -> List(businessKey2 -> conditionalProb))
      }
      .reduceByKey(_ ++ _)
      .collect().toMap)

    val rankedBusinessesByNumberOfCustomers = sc.broadcast(
      businessKeyToCustomerSet.mapValues(_.size).sortBy(_._2, ascending = false).collect().toList)

    filteredCustomersAndBusinesses.unpersist()
    businessKeyToCustomerSet.unpersist()


    new Recommender {
      // returns customerId -> List[(merchantName, merchantTown)]
      def recommendations(customers: RDD[Long], n: Int): RDD[(Long, List[(String, String)])] = {
        customers.flatMap(customerId =>
          customerIdToBusinessSet.value.get(customerId)
          .map(customerBusinessKeys => for {
            customerBusinessKeys <- customerIdToBusinessSet.value.get(customerId).toList
            customerBusinessKey <- customerBusinessKeys
            similarityRow <- item2itemMatrix.value.get(customerBusinessKey).toList
            (similarBusiness, conditionalProb) <- similarityRow
            if !customerBusinessKeys.contains(similarBusiness)
          } yield (customerId, similarBusiness) -> conditionalProb
          ).getOrElse(rankedBusinessesByNumberOfCustomers.value.take(n).map {
            case (businessKey, rank) => (customerId, businessKey) -> rank.toDouble
          }
          ))
        .reduceByKey(_ + _)
        .groupBy(_._1._1)
        .mapValues(_.map {
          case ((_, businessKey), conditionalProb) => businessKey -> conditionalProb
        })
        .mapValues(businessesScores => {
          val recommendations = TopElements.topN(businessesScores)(_._2, n).map(_._1)
          if (recommendations.size >= n) recommendations
          else (recommendations ++
            (rankedBusinessesByNumberOfCustomers.value.take(n).map(_._1).toSet -- recommendations)).take(n)
        })
      }
    }
  }
}
