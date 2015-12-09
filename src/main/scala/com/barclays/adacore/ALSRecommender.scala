package com.barclays.adacore

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.jblas.DoubleMatrix

case class ALSRecommender(@transient sc: SparkContext, rank: Int, numIterations: Int, alpha: Double, blocks: Int,
                          lambda: Double, maxRecommendations: Int, sampleFraction: Double) extends Recommender {
  // returns customerId -> List[(merchantName, merchantTown)]
  def recommendations(data: RDD[AnonymizedRecord]): RDD[(Long, List[(String, String)])] = {
    val businessKeyToId = data.map(_.businessKey).distinct().zipWithUniqueId().mapValues(_.toInt).collect().toMap

    val businessIdToKey = businessKeyToId.map(_.swap)

    val businessIdBV = sc.broadcast(businessKeyToId)
    val businessKeyBV = sc.broadcast(businessIdToKey)

    val ratings =
      data.map(record => (record.maskedCustomerId.toInt, businessIdBV.value(record.businessKey)) -> 1)
      .reduceByKey(_ + _)
      .map {
        case ((customerId, businessId), count) => Rating(customerId, businessId, count.toDouble)
      }

    val model = ALS.trainImplicit(ratings, rank, numIterations, lambda, blocks, alpha)

    val businessFeatures: Broadcast[Map[Int, Array[Double]]] = sc.broadcast(model.productFeatures.collect().toMap)
    val userFeatures: Broadcast[Map[Int, Array[Double]]] = sc.broadcast(model.userFeatures.collect().toMap)

    data.keyBy(_.maskedCustomerId.toInt).mapValues(record => businessIdBV.value(record.businessKey))
    .distinct().mapValues(Set(_)).reduceByKey(_ ++ _).sample(false, sampleFraction)
    .map {
      case (customerId, trainingBusinesses) =>
        //TODO: apply some pre-filtering
        val recommendableBusinesses = businessKeyBV.value.keySet -- trainingBusinesses
        val userVector = new DoubleMatrix(userFeatures.value(customerId))

        customerId.toLong ->
          recommendableBusinesses.foldLeft((Set.empty[(Int, Double)], Double.MaxValue)) {
            // TODO: refactor with topN
            case (accumulator@(topBusinesses, minScore), businessId) =>
              val businessVector = new DoubleMatrix(businessFeatures.value(businessId))
              val score = userVector.dot(businessVector)
              if (topBusinesses.size < maxRecommendations)
                (topBusinesses + (businessId -> score), math.min(minScore, score))
              else if (score > minScore) {
                val newTopBusinesses = topBusinesses - topBusinesses.minBy(_._2) + (businessId -> score)
                (newTopBusinesses, newTopBusinesses.map(_._2).min)
              }
              else accumulator
          }
          ._1.toList.sortBy(_._2).reverse.map(_._1).map(businessId => businessKeyBV.value(businessId))
    }
  }
}
