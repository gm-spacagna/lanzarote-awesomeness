package com.barclays.adacore.model

import com.barclays.adacore.model.LanzaroteBest
import com.barclays.adacore.{RecommenderTrainer, AnonymizedRecord, Recommender}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Matrix

import scala.collection.immutable.IndexedSeq
import scalaz.Scalaz._

case object LanzaroteCoach extends RecommenderTrainer {
  override def train(data: RDD[AnonymizedRecord]): Recommender = {
    val (features, history) = Covariance.features(data)
    val (cov, idx) = features |> Covariance.toCovariance
    LanzaroteBest(cov, idx, history)
  }

}

case class LanzaroteBest(knowledge: Matrix, idx: Map[(String, String), Int], userHistory: RDD[(Long, List[(String, String)])])
  extends Recommender {

  def getBestForId(rowId: Int): Int = {
    (for {
      j <- (0 to knowledge.numCols - 1)
      v = knowledge.apply(rowId, j)
    } yield (v, j))
    .foldLeft((-1.0, -1))((acc, vi) => if (acc._1 < vi._1) vi else acc)._2
  }

  override def recommendations(customers: RDD[Long], n: Int): RDD[(Long, List[(String, String)])] =
      customers.map(userHistory.lookup(_)).map(l => l.head.map(k => getBestForId(idx(k)))))

}
