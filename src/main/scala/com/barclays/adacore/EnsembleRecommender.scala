package com.barclays.adacore

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.parallel.ParMap

case class EnsembleRecommender(@transient sc: SparkContext, recommenderTrainerList: List[RecommenderTrainer])
extends RecommenderTrainer {
  def train(data: RDD[AnonymizedRecord]): Recommender = {

    // output of each recommender: RDD[(Long, List[(String, String)])]

    val recommenderList: List[Recommender] = recommenderTrainerList.map(_.train(data))

    new Recommender {
      // returns customerId -> List[(merchantName, merchantTown)]
      def recommendations(customers: RDD[Long], n: Int): RDD[(Long, List[(String, String)])] = {
        recommenderList
          .map(_.recommendations(customers, n).mapValues(_.zipWithIndex)).reduce(_ union _).reduceByKey(_ ++ _)
          .mapValues(_.groupBy(_._1)
            .mapValues(businessesWithIndex => (businessesWithIndex.size, businessesWithIndex.map(_._2).map(n - _).sum))
            .toList.sortBy(_._2).reverse.map(_._1).take(n)
          )
      }
    }
  }
}
