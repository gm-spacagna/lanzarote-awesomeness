package com.barclays.adacore.model

import breeze.collection.mutable.SparseArray
import com.barclays.adacore.AnonymizedRecord
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import breeze.linalg._
import breeze.linalg.SparseVector
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.linalg.Matrix

import scala.collection.immutable.IndexedSeq
import scalaz.Scalaz._

case object Covariance {

  def features(records: RDD[AnonymizedRecord])
              (implicit minKTx: Int = 10, minAmountPerMerchant: Double = 100.0, minCustomersPerMerchant: Int = 7):
  (RDD[((String, String), SparseVector[Double])], RDD[(Long, Set[(String, String)])]) = {
    val entries: RDD[((Long, (String, String)), (Int, Double))] =
      records.keyBy(r => (r.maskedCustomerId, ETL.businessID(r)))
      .mapValues(value => (1, value.amount))
      .reduceByKey(_ |+| _)
    //      .filter(v => v._2._1 > minKTx & v._2._2 == minAmountPerMerchant)

    println("FEATURES>>ENTRIES " + entries.count() + "  DATA " + records.count())

    val businessIdx = entries.map(_._1._2).distinct.zipWithIndex.mapValues(_.toInt).collect().toMap

    println("FEATURES>>BUSINESS_IDX " + businessIdx.size)
    val custIdx: Broadcast[Map[Long, Long]] = records.context.broadcast(entries.map(_._1._1).distinct().zipWithIndex().collect().toMap)

    println("FEATURES>>CUST_IDX " + custIdx.value.size)

    val history: RDD[(Long, Set[(String, String)])] =
      entries.map(e => e._1._1 -> List((e._1._2, e._2)))
      .reduceByKey(_ |+| _)
      .mapValues(v => v.filter(_._2._1 > minKTx).sortBy(el => -el._2._2).map(_._1).toSet)

    println("FEATURES>>HISTORY " + history.count())

    (entries.keyBy(_._1._2)
     .mapValues(v => List((v._1._1, v._2)))
     .reduceByKey(_ |+| _)
     .mapPartitions(part => {
       val custIdxBV: Map[Long, Long] = custIdx.value

       //       part.filter(_._2.size > minCustomersPerMerchant)
       part
       .map(e => {
         val (bId, col) = e
         val mapIdxVal = col.map(el => custIdxBV(el._1).toInt -> el._2._1.toDouble)
                         .sortBy(_._1).toArray
         val sv = new SparseVector[Double](mapIdxVal.map(_._1), mapIdxVal.map(_._2), custIdxBV.size)
         (bId, sv)
       })
     }), history)
  }

  def toCovarianceScore(sc: SparkContext)
                       (features: Map[(String, String), SparseVector[Double]]): Map[(String, String), List[((String, String), Double)]] = {
    val keys: Array[(String, String)] = features.keys.toArray
    val numFeatures = keys.length
    println("FEATURE KEYS: " + numFeatures)

    val pairs: IndexedSeq[((String, String), (String, String))] = for {
      cur <- 0 to (numFeatures - 1)
      other <- (cur + 1) to (numFeatures - 1)
    } yield (keys(cur), keys(other))

    val pairsRDD = sc.parallelize(pairs, numSlices = 10000)
    println("PAIRS :" + pairs.size)

    pairsRDD.flatMap(p => p match {
      case (l, r) =>
        val v1 = features(l)
        val v2 = features(r)
        val res = v1.t * v2 / (v1.norm[SparseVector[Double], Double]() * v2.norm[SparseVector[Double], Double]())
        List((l, r) -> res, (r, l) -> res)
    })
    .map(e => e._1._2 -> List((e._1._1, e._2)))
    .reduceByKey(_ ++ _)
    .map(e => (e._1, e._2.sortBy(-_._2)))
    .collect
    .toMap
  }
}
