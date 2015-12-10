package com.barclays.adacore.model

import com.barclays.adacore.utils.Logger
import com.barclays.adacore.{RecommenderTrainer, AnonymizedRecord, Recommender}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Matrix

import scala.collection.immutable.IndexedSeq
import scalaz.Scalaz._

class LanzaroteCoach() extends RecommenderTrainer {
  override def train(data: RDD[AnonymizedRecord]): Recommender = {
    val (features, history) = Covariance.features(data)
    println("FEATURES " + features.size + "   HISTORY " + history.count())
    val scores: RDD[((String, String), List[((String, String), Double)])] = Covariance.toCovarianceScore(data.context)(features)
    LanzaroteBest(scores, history)
  }
}

case class LanzaroteBest(scores: RDD[((String, String), List[((String, String), Double)])],
                         history: RDD[(Long, List[(String, String)])]) extends Recommender {

  override def recommendations(customers: RDD[Long], n: Int): RDD[(Long, List[(String, String)])] = {
    val defaultRecommendations = scores.keys.take(n).toIterable

    println("PRINTING SCORES: " + scores.count())
    scores.foreach(println)
    Logger.infoLevel.info("=======================")
    Logger.infoLevel.info("PRINTING DEFAULTS")
    defaultRecommendations.foreach(println)
    Logger.infoLevel.info("=======================")

    customers
    .map(e => e -> None)
    .cogroup(history)
    .map(e => e._1 -> e._2._2.flatten)
    .flatMap(v => {
      if (v._2.nonEmpty)
        v._2.map(b => b -> v._1)
      else
        defaultRecommendations.map(b => b -> v._1)
    })
    .cogroup(scores)
    .map { s =>
      Logger.infoLevel.info(s)
      s._2._1.head ->
        List(s._2._2.flatten.head._1)
    }
    .reduceByKey(_ |+| _)
  }
}
