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

  def evaluate(recommender: Recommender,
               trainingData: RDD[AnonymizedRecord],
               testData: RDD[AnonymizedRecord], n: Int = 100): Double = {
    val recommendations: RDD[(Long, List[(String, String)])] = recommender.recommendations(trainingData).cache()
    val evaluation: RDD[(Long, Set[(String, String)])] =
      testData.keyBy(_.maskedCustomerId).mapValues(_.businessKey).distinct().mapValues(Set(_)).reduceByKey(_ ++ _)
    MAP(n, recommendations, evaluation)
  }
}

trait Recommender {
  // returns customerId -> List[(merchantName, merchantTown)]
  def recommendations(data: RDD[AnonymizedRecord]): RDD[(Long, List[(String, String)])]
}

case class RandomRecommender(@transient sc: SparkContext, n: Int) extends Recommender {
  def recommendations(data: RDD[AnonymizedRecord]): RDD[(Long, List[(String, String)])] = {

    val businessesBV = sc.broadcast(data.map(_.businessKey).distinct().collect().toSet)

    data.keyBy(_.maskedCustomerId).mapValues(_.businessKey).distinct().mapValues(Set(_)).reduceByKey(_ ++ _)
    .mapValues(trainingBusinesses => (businessesBV.value -- trainingBusinesses).take(n).toList)
  }
}
