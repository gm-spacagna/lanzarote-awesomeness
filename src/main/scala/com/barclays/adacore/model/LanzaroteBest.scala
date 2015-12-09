package com.barclays.adacore.model

import com.barclays.adacore.{RecommenderTrainer, AnonymizedRecord, Recommender}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Matrix

import scala.collection.immutable.IndexedSeq
import scalaz.Scalaz._

class LanzaroteCoach() extends RecommenderTrainer {
  override def train(data: RDD[AnonymizedRecord]): Recommender = {
    val (features, history) = Covariance.features(data)
    val scores: RDD[((String, String), List[((String, String), Double)])] = features |> Covariance.toCovarianceScore
    LanzaroteBest(scores, history)
  }
}

case class LanzaroteBest(scores: RDD[((String, String), List[((String, String), Double)])],
                         history: RDD[(Long, List[(String, String)])]) extends Recommender {

  override def recommendations(customers: RDD[Long], n: Int): RDD[(Long, List[(String, String)])] =
    customers.map(c =>
      c -> history.lookup(c) match {
        case (_, res) if res.isEmpty =>
          c -> scores.take(n).map(_._2.head._1).toList

        case (_, res :: Nil) if res.size == 1 =>
          c -> res.flatMap(k => scores.lookup(k).head.take(n).map(_._1))
      })
}
