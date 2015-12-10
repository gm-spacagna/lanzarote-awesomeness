package com.barclays.adacore

import com.barclays.adacore.utils.TopElements
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scalaz.Scalaz._

case class RandomForestRecommender(@transient sc: SparkContext, minNumTransactions: Int) extends RecommenderTrainer {
  def train(data: RDD[AnonymizedRecord]): Recommender = {
    ???
  }
}
