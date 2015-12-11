package com.barclays.adacore

import com.barclays.adacore.utils.TopElements
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, ALS, Rating}
import org.apache.spark.rdd.RDD
import org.jblas.DoubleMatrix

case class ALSRecommender(@transient sc: SparkContext, rank: Int, numIterations: Int, alpha: Double, blocks: Int,
                          lambda: Double, maxRecommendations: Int) extends RecommenderTrainer {
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

    val businessKeyToId: Map[(String, String), Int] = data.map(_.businessKey).distinct().zipWithUniqueId().mapValues(_.toInt).collect().toMap

    val businessIdBV: Broadcast[Map[(String, String), Int]] = sc.broadcast(businessKeyToId)

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

    import scalaz.Scalaz._
    new Recommender {
      // returns customerId -> List[(merchantName, merchantTown)]
      def recommendations(customers: RDD[Long], n: Int): RDD[(Long, List[(String, String)])] =
        customers.map(customerId =>
          customerId ->
            ((customerIdToBusinessSet.value.get(customerId) match {
              case Some(customerBusinessKeys) =>
                val recommendableBusinesses = businessIdBV.value.keySet -- customerBusinessKeys
                val userVector = new DoubleMatrix(userFeatures.value(customerId.toInt))
                (for {
                  businessKey <- recommendableBusinesses
                  businessVector = new DoubleMatrix(businessFeatures.value(businessIdBV.value(businessKey)))
                  score = userVector.dot(businessVector)
                } yield businessKey -> score)
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

