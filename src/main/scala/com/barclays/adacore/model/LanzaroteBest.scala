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
    val (features, history) = Covariance.features(data.coalesce(16, shuffle = false))
    println("FEATURES " + features.size + "   HISTORY " + history.count())
    val scores: Map[(String, String), List[((String, String), Double)]] = Covariance.toCovarianceScore(data.context)(features)
    LanzaroteBest(scores, history)
  }
}

case class LanzaroteBest(scores: Map[(String, String), List[((String, String), Double)]], history: RDD[(Long, Set[(String, String)])]) extends Recommender {

  override def recommendations(customers: RDD[Long], n: Int): RDD[(Long, List[(String, String)])] = {
    val defaultRecommendations = scores.keys.take(n).toIterable

    //    println("PRINTING SCORES: " + scores.size())
    //    scores.foreach(println)
    //    Logger.infoLevel.info("=======================")
    //    Logger.infoLevel.info("PRINTING DEFAULTS")
    //    defaultRecommendations.foreach(println)
    //    Logger.infoLevel.info("=======================")

    customers
    .map(e => e -> None)
    .cogroup(history)
    .map(e => e._1 -> (e._2._2.head match {
      case visited if visited.nonEmpty =>
        visited.flatMap(ks => scores(ks).filter(cand => !visited.contains(cand._1)))
        .groupBy(_._1)
        .map(cand => cand._2.maxBy(_._2))
        .toList.sortBy(-_._2)
        .take(n)
        .map(_._1)

      case _ =>
        defaultRecommendations.toList
    }))
  }
}
