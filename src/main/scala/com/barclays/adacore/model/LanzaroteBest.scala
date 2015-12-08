package com.barclays.adacore.model

import com.barclays.adacore.{AnonymizedRecord, Recommender}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class LanzaroteBest(path: String)(@transient sc: SparkContext, n: Int) extends Recommender {

  // returns customerId -> List[(merchantName, merchantTown)]
  override def recommendations(data: RDD[AnonymizedRecord]): RDD[(Long, List[(String, String)])] = {}
}
