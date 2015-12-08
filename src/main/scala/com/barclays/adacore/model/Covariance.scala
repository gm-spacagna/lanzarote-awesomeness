package com.barclays.adacore.model

import com.barclays.adacore.AnonymizedRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{Matrix, Vector, Vectors, SparseVector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

import scalaz.Scalaz._

case object Covariance {

  def features(records: RDD[AnonymizedRecord]): RDD[(String, Vector)] = {
    val entries = records.keyBy(r => (r.maskedCustomerId, ETL.businessID(r))).mapValues(value => 1).reduceByKey(_ + _)
    val businessIdx = entries.map(_._1._2).distinct.zipWithIndex.mapValues(_.toInt).collect().toMap

    val custIdx: Broadcast[Map[Long, Long]] = records.context.broadcast(entries.map(_._1._1).distinct().zipWithIndex().collect().toMap)

    entries.keyBy(_._1._2).mapValues(v => List((v._1._1, v._2)))
    .reduceByKey(_ |+| _)
    .mapPartitions(part => {
      val custIdxBV: Map[Long, Long] = custIdx.value

      part.map(e => {
        val (bId, col) = e
        val sv = Vectors.sparse(col.size, col.map(el => custIdxBV(el._1).toInt -> el._2.toDouble))
        (bId, sv)
      })
    })
  }

  def toCovariance(features: RDD[(String, Vector)]) = {
    val data = new RowMatrix(features.map(_._2))
    data.computeCovariance()
  }

}
