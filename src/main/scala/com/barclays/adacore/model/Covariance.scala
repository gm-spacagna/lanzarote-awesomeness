package com.barclays.adacore.model

import breeze.collection.mutable.SparseArray
import com.barclays.adacore.AnonymizedRecord
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
  (RDD[((String, String), SparseVector[Double])], RDD[(Long, List[(String, String)])]) = {
    val entries: RDD[((Long, (String, String)), (Int, Double))] =
      records.keyBy(r => (r.maskedCustomerId, ETL.businessID(r)))
      .mapValues(value => (1, value.amount))
      .reduceByKey(_ |+| _)
      .filter(v => v._2._1 > minKTx & v._2._2 == minAmountPerMerchant)

    val businessIdx = entries.map(_._1._2).distinct.zipWithIndex.mapValues(_.toInt).collect().toMap

    val custIdx: Broadcast[Map[Long, Long]] = records.context.broadcast(entries.map(_._1._1).distinct().zipWithIndex().collect().toMap)

    val history: RDD[(Long, List[(String, String)])] =
      entries.map(e => e._1._1 -> List((e._1._2, e._2)))
      .reduceByKey(_ |+| _)
      .mapValues(v => v.filter(_._2._1 > minKTx).sortBy(el => -el._2._2).map(_._1))

    (entries.keyBy(_._1._2)
     .mapValues(v => List((v._1._1, v._2)))
     .reduceByKey(_ |+| _)
     .mapPartitions(part => {
       val custIdxBV: Map[Long, Long] = custIdx.value

       part.filter(_._2.size > minCustomersPerMerchant)
       .map(e => {
         val (bId, col) = e
         val mapIdxVal = col.map(el => custIdxBV(el._1).toInt -> el._2._1.toDouble)
                         .sortBy(_._1).toArray
         val sv = new SparseVector[Double](mapIdxVal.map(_._1), mapIdxVal.map(_._2), custIdxBV.size)
         (bId, sv)
       })
     }), history)
  }

  def toCovarianceScore(features: RDD[((String, String), SparseVector[Double])]): RDD[((String, String), List[((String, String), Double)])] = {
    val keys: Array[(String, String)] = features.keys.collect
    val numFeatures = keys.length

    val pairs: IndexedSeq[((String, String), (String, String))] = for {
      cur <- 0 to (numFeatures - 1)
      other <- (cur + 1) to (numFeatures - 1)
    } yield (keys(cur), keys(other))

    val pairsBV = features.context.broadcast(pairs)

    features.map(el => (el._1, el._2 - DenseVector.fill[Double](el._2.size)(el._2.sum / el._2.size))) //- (el._2.values.sum / el._2.size)
    .mapPartitions(part => {
      val pairsPart = pairsBV.value

      part.flatMap(el => {
        pairsPart.flatMap(_ match {
          case (l, r) if l == el._1 => Some((l, r), List(Left(el._2)))
          case (l, r) if r == el._1 => Some((l, r), List(Right(el._2)))
          case _ => None
        })
      })
    })
    .reduceByKey(_ |+| _)
    .flatMap(p => {
      p match {
        case (k, Left(v1) :: Right(v2) :: Nil) =>
          val res = v1.t * v2
          Some(List(k -> res, k.swap -> res))

        case (k, Right(v1) :: Left(v2) :: Nil) =>
          val res = v1.t * v2
          Some(List(k -> res, k.swap -> res))

        case (k, badList) =>
          println(">>>>>>>>>>>>>>>>>>>WHAT THE HECK?<<<<<<<<<<<<<<<<<<<<<")
          None
      }
    })
    .flatMap(identity)
    .map(e => e._1._1 -> List((e._1._2, e._2)))
    .reduceByKey(_ |+| _)
    .map(e => (e._1, e._2.sortBy(-_._2)))
  }
}
