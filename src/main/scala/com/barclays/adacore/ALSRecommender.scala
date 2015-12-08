package com.barclays.adacore

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

case class ALSRecommender(@transient sc: SparkContext, rank: Int, numIterations: Int, alpha: Double, blocks: Int,
                          lambda: Double) extends Recommender {
  // returns customerId -> List[(merchantName, merchantTown)]
  def recommendations(data: RDD[AnonymizedRecord]): RDD[(Long, List[(String, String)])] = {
    val businessKeyToId = data.map(_.businessKey).distinct().zipWithUniqueId().mapValues(_.toInt).collect().toMap

    val businessIdToKey = businessKeyToId.map(_.swap)

    val businessIdBV = sc.broadcast(businessKeyToId)
    val businessKeyBV = sc.broadcast(businessIdToKey)

    val ratings =
      data.map(record => (record.maskedCustomerId.toInt, businessIdBV.value(record.businessKey)) -> 1).reduceByKey(_ + _)
      .map {
        case ((customerId, businessId), count) => Rating(customerId, businessId, count.toDouble)
      }

    val model = ALS.trainImplicit(ratings, rank, numIterations, lambda, blocks, alpha)

    val usersBusinesses =
      data.keyBy(_.maskedCustomerId).mapValues(_.businessKey).distinct().mapValues(Set(_)).reduceByKey(_ ++ _)
      .flatMap {
        case (customerId, trainingBusinesses) =>
          (businessIdBV.value.keySet -- trainingBusinesses).map(customerId.toInt -> businessIdBV.value(_))
      }

    model.predict(usersBusinesses).groupBy(_.user.toLong)
    .mapValues(ratings => ratings.toList.sortBy(_.rating).reverse.map(rating => businessKeyBV.value(rating.product)))
  }
}
