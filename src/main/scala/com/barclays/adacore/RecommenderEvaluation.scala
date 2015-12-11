package com.barclays.adacore

import com.barclays.adacore.utils.MAP
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.util.Random

case class RecommenderEvaluation(@transient sc: SparkContext) {
  def splitData(data: RDD[AnonymizedRecord],
                fraction: Double = 0.8): (RDD[AnonymizedRecord], RDD[AnonymizedRecord]) = {
    val customerIdToTestBusinesses = sc.broadcast(
      data.keyBy(_.maskedCustomerId).mapValues(_.businessKey).distinct().mapValues(Set(_)).reduceByKey(_ ++ _)
      .mapValues(businesses => businesses.filter(_ => Random.nextDouble() > fraction)).collect().toMap
    )

    val recordsWithTestLabel: RDD[(AnonymizedRecord, Boolean)] =
      data.map(record =>
        record -> customerIdToTestBusinesses.value(record.maskedCustomerId).contains(record.businessKey)
      )

    (recordsWithTestLabel.filter(!_._2).keys, recordsWithTestLabel.filter(_._2).keys)
  }

  def evaluate(recommenderTrainer: RecommenderTrainer,
               trainingData: RDD[AnonymizedRecord],
               testData: RDD[AnonymizedRecord],
               n: Int = 100, evaluationSamplingFraction: Double): Double = {
    val testCustomers = testData.map(_.maskedCustomerId).distinct().sample(false, evaluationSamplingFraction).cache()

    val recommendations: RDD[(Long, List[(String, String)])] =
      recommenderTrainer.train(trainingData)
      .recommendations(testCustomers, n)
      .cache()

    assert(recommendations.keys.collect().toSet == testCustomers.collect().toSet,
      "testCustomers is not equals to the recommendation customers")

    assert(recommendations.filter(_._2.size != n).count == 0,
      "not all of the customer recommendations have exactly " + n + "  businesses")

    val evaluation: RDD[(Long, Set[(String, String)])] =
      testData.keyBy(_.maskedCustomerId).mapValues(_.businessKey).distinct().mapValues(Set(_)).reduceByKey(_ ++ _)
    MAP(n, recommendations, evaluation)
  }
}

class RecommenderWithPrecomputedRecommendationsAndMostPopularAsDefault(preComputedRecommendationsBV: Broadcast[Map[Long, List[(String, String)]]],
                                                                       rankedBusinessesByNumberOfCustomersBV: Broadcast[List[(String, String)]]) extends Recommender {
  def recommendations(customers: RDD[Long], n: Int): RDD[(Long, List[(String, String)])] = {
    customers.map(customerId => customerId -> {
      val recommendations =
        preComputedRecommendationsBV.value.getOrElse(customerId, rankedBusinessesByNumberOfCustomersBV.value)
        .take(n)
      if (recommendations.size >= n) recommendations
      else (recommendations ++
        (rankedBusinessesByNumberOfCustomersBV.value.take(n).toSet -- recommendations)).take(n)
    })
  }
}

trait Recommender extends Serializable {
  // returns customerId -> List[(merchantName, merchantTown)]
  def recommendations(customers: RDD[Long], n: Int): RDD[(Long, List[(String, String)])]
}

trait RecommenderTrainer {
  def train(data: RDD[AnonymizedRecord]): Recommender

  def mostPopularBusinesses(data: RDD[AnonymizedRecord]): RDD[(String, String)] =
    data.map(record => (record.businessKey, record.maskedCustomerId)).distinct()
    .mapValues(_ => 1).reduceByKey(_ + _).sortBy(_._2, ascending = false).keys

  def customerIdToBusinessKeySet(data: RDD[AnonymizedRecord]): RDD[(Long, Set[(String, String)])] =
    data.map(record => (record.maskedCustomerId, record.businessKey)).distinct()
    .groupByKey().mapValues(_.toSet)

  def businessKeyToCustomerIdSet(data: RDD[AnonymizedRecord]): RDD[((String, String), Set[Long])] =
    data.map(record => (record.maskedCustomerId, record.businessKey)).map(_.swap).distinct()
    .groupByKey().mapValues(_.toSet)
}

case class RandomRecommender(@transient sc: SparkContext) extends RecommenderTrainer {
  def train(data: RDD[AnonymizedRecord]): Recommender = {
    val businessesBV = sc.broadcast(data.map(_.businessKey).distinct().collect().toSet)

    val customerIdToTrainingBusinesses = sc.broadcast(
      data.keyBy(_.maskedCustomerId).mapValues(_.businessKey).distinct().mapValues(Set(_))
      .reduceByKey(_ ++ _).collect().toMap
    )

    new Recommender {
      // returns customerId -> List[(merchantName, merchantTown)]
      def recommendations(customers: RDD[Long], n: Int): RDD[(Long, List[(String, String)])] =
        customers.map(customerId => customerId -> customerIdToTrainingBusinesses.value.getOrElse(customerId, Set.empty))
        .mapValues(trainingBusinesses => (businessesBV.value -- trainingBusinesses).take(n).toList)
    }
  }
}
