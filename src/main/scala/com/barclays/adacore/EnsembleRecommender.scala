package com.barclays.adacore

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class EnsembleRecommender(@transient sc: SparkContext, recommenderList: List[List[(Long, List[(String, String)]]])
extends RecommenderTrainer {
  def train(data: RDD[AnonymizedRecord]): Recommender = {

    // output of each recommender: RDD[(Long, List[(String, String)])]
    val user2Allretailers: RDD[(Long, List[(String, String)])] = sc.parallelize(recommenderList.flatten).groupByKey()
      .map({
       case (userId,listOfListsOfRetailers) => (userId,listOfListsOfRetailers.toList.flatten)
      })

    val user2RetailerFreq: RDD[(Long, List[((String, String), Int)])] = user2Allretailers.map({
      case (userId,listOfBusinesses) => (userId,listOfBusinesses.groupBy(businessId => businessId)
                                        .map(businessAggr => (businessAggr._1,businessAggr._2.length)).toList
                                        .sortBy(- _._2))
                                        })

    new Recommender {
      // returns customerId -> List[(merchantName, merchantTown)]
      def recommendations(customers: RDD[Long], n: Int): RDD[(Long, List[(String, String)])] =
        user2RetailerFreq.map({
          case (user, sortedRetailerFreq) => (user,sortedRetailerFreq.map(_._1).take(n))
        })
    }
  }
}
