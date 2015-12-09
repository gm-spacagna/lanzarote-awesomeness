package com.barclays.adacore.model

import com.barclays.adacore.AnonymizedRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{Matrix, Vector, Vectors, SparseVector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

import scalaz.Scalaz._

case object Covariance {

  def features(records: RDD[AnonymizedRecord])(implicit minKTx: Int = 5): (RDD[((String, String), Vector)], RDD[(Long, List[(String, String)])]) = {
    val entries: RDD[((Long, (String, String)), (Int, Double))] =
      records.keyBy(r => (r.maskedCustomerId, ETL.businessID(r)))
      .mapValues(value => (1, value.amount))
      .reduceByKey(_ |+| _)

    val businessIdx = entries.map(_._1._2).distinct.zipWithIndex.mapValues(_.toInt).collect().toMap

    val custIdx: Broadcast[Map[Long, Long]] = records.context.broadcast(entries.map(_._1._1).distinct().zipWithIndex().collect().toMap)

    val history: RDD[(Long, List[(String, String)])] =
      entries.map(e => e._1._1 -> List((e._1._2, e._2)))
      .reduceByKey(_ |+| _)
      .mapValues(v => v.filter(_._2._1 > minKTx).sortBy(el => -el._2._2).map(_._1))

    (entries.keyBy(_._1._2).mapValues(v => List((v._1._1, v._2)))
     .reduceByKey(_ |+| _)
     .mapPartitions(part => {
       val custIdxBV: Map[Long, Long] = custIdx.value

       part.map(e => {
         val (bId, col) = e
         val sv = Vectors.sparse(custIdxBV.size, col.map(el => custIdxBV(el._1).toInt -> el._2._1.toDouble))
         (bId, sv)
       })
     }), history)
  }

  def toCovariance(features: RDD[((String, String), Vector)]): (Matrix, Map[(String, String), Int]) = {
    val data = new RowMatrix(features.map(_._2))
    (data.computeCovariance(), features.map(_._1).collect().zipWithIndex.toMap)
  }
}
