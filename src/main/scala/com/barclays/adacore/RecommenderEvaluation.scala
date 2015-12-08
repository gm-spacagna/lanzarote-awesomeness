package com.barclays.adacore

import com.barclays.adacore.utils.MAP
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.Random

case class RecommenderEvaluation(@transient sc: SparkContext) {
  // Map[customerId -> (Set[trainingBusinesses], Set[testBusinesses])] such that the two sets intersection is empty
  //  def evaluationBusinessMaps(trainingData: RDD[AnonymizedRecord],
  //                             testData: RDD[AnonymizedRecord]): RDD[(Long, (Set[(String, String)], Set[(String, String)]))] = ???

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

  def evaluate(recommender: RecommenderTrainer,
               trainingData: RDD[AnonymizedRecord],
               testData: RDD[AnonymizedRecord],
               n: Int = 100, evaluationSamplingFraction: Double): Double = {
    val recommendations: RDD[(Long, List[(String, String)])] =
      recommender.train(trainingData)
      .recommendations(testData.map(_.maskedCustomerId).distinct().sample(false, evaluationSamplingFraction), n)
      .cache()
    val evaluation: RDD[(Long, Set[(String, String)])] =
      testData.keyBy(_.maskedCustomerId).mapValues(_.businessKey).distinct().mapValues(Set(_)).reduceByKey(_ ++ _)
    MAP(n, recommendations, evaluation)
  }
}

trait Recommender extends Serializable {
  // returns customerId -> List[(merchantName, merchantTown)]
  def recommendations(customers: RDD[Long], n: Int): RDD[(Long, List[(String, String)])]
}

trait RecommenderTrainer {
  def train(data: RDD[AnonymizedRecord]): Recommender
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
        customers.map(customerId => customerId -> customerIdToTrainingBusinesses.value(customerId))
        .mapValues(trainingBusinesses => (businessesBV.value -- trainingBusinesses).take(n).toList)
    }
  }
}