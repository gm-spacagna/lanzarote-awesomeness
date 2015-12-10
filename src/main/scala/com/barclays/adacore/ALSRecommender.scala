package com.barclays.adacore

import com.barclays.adacore.RecommenderTrainer
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, ALS, Rating}
import org.apache.spark.rdd.RDD
import org.jblas.DoubleMatrix

case class ALSRecommender(@transient sc: SparkContext, rank: Int, numIterations: Int, alpha: Double, blocks: Int,
                          lambda: Double, maxRecommendations: Int, sampleFraction: Double) extends RecommenderTrainer {
  // returns customerId -> List[(merchantName, merchantTown)]

  def train(data: RDD[AnonymizedRecord]): Recommender = {

    val filteredCustomersAndBusinesses: RDD[(Long, (String, String))] =
      data.map(record => (record.maskedCustomerId, record.businessKey) -> 1).reduceByKey(_ + _).keys.cache()

    val customerIdToBusinessSet: Broadcast[Map[Long, Set[(String, String)]]] =
      sc.broadcast(filteredCustomersAndBusinesses.mapValues(Set(_)).reduceByKey(_ ++ _).collect().toMap)

    val businessKeyToCustomerSet: RDD[((String, String), Set[Long])] =
      filteredCustomersAndBusinesses.map(_.swap).mapValues(Set(_)).reduceByKey(_ ++ _).cache()

    val rankedBusinessesByNumberOfCustomers = sc.broadcast(
      businessKeyToCustomerSet.mapValues(_.size.toDouble).sortBy(_._2, ascending = false).collect().toList
    )

    filteredCustomersAndBusinesses.unpersist()
    businessKeyToCustomerSet.unpersist()

    // training should be outside
    val businessKeyToId: Map[(String, String), Int] = data.map(_.businessKey).distinct().zipWithUniqueId().mapValues(_.toInt).collect().toMap

    val businessIdToKey: Map[Int, (String, String)] = businessKeyToId.map(_.swap)

    val businessIdBV: Broadcast[Map[(String, String), Int]] = sc.broadcast(businessKeyToId)
    val businessKeyBV: Broadcast[Map[Int, (String, String)]] = sc.broadcast(businessIdToKey)

    // training procedure
    val ratings =
      data.map(record => (record.maskedCustomerId.toInt, businessIdBV.value(record.businessKey)) -> 1)
        .reduceByKey(_ + _)
        .map {
          case ((customerId, businessId), count) => Rating(customerId, businessId, count.toDouble)
        }

    val model: MatrixFactorizationModel = ALS.trainImplicit(ratings, rank, numIterations, lambda, blocks, alpha)

    // model params
    val businessFeatures: Broadcast[Map[Int, Array[Double]]] = sc.broadcast(model.productFeatures.collect().toMap)
    val userFeatures: Broadcast[Map[Int, Array[Double]]] = sc.broadcast(model.userFeatures.collect().toMap)


    new Recommender {

      def recommendations(customers: RDD[Long], n: Int): RDD[(Long, List[(String, String)])] = {

        customers.map(customerId =>
          customerId ->
            ((customerIdToBusinessSet.value.get(customerId) match {
              ???
            }


              //val a: RDD[(Long, List[(String, String)])] =
        data.keyBy(_.maskedCustomerId.toInt).mapValues(record => businessIdBV.value(record.businessKey))
          .distinct().mapValues(Set(_)).reduceByKey(_ ++ _).sample(false, sampleFraction)
          .map {
            case (customerId, trainingBusinesses) =>
              //TODO: apply some pre-filtering
              val recommendableBusinesses = businessKeyBV.value.keySet -- trainingBusinesses
              val userVector = new DoubleMatrix(userFeatures.value(customerId))

              customerId.toLong ->
                recommendableBusinesses.foldLeft((Set.empty[(Int, Double)], Double.MaxValue)) {
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

  }
}

