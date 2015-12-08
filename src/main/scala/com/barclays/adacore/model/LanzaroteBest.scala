package com.barclays.adacore.model

import com.barclays.adacore.{AnonymizedRecord, Recommender}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Matrix

import scalaz.Scalaz._

trait RecommenderTrainer {
  def train(data: RDD[AnonymizedRecord]): Recommender
}

case object LanzaroteCoach extends RecommenderTrainer {
  override def train(data: RDD[AnonymizedRecord])(@transient sc: SparkContext, n: Int): Recommender =
    LanzaroteBest(Covariance.features(data) |> Covariance.toCovariance)(sc, n)
}

case class LanzaroteBest(knowledge: Matrix)(@transient sc: SparkContext, n: Int) extends Recommender {

  // returns customerId -> List[(merchantName, merchantTown)]
  def recommendations(data: RDD[Long]): RDD[(Long, List[(String, String)])] = ???
}
