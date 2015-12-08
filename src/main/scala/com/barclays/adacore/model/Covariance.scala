package com.barclays.adacore.model

import com.barclays.adacore.AnonymizedRecord
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

case object Covariance {

  def features(records: RDD[AnonymizedRecord]) : RDD[SparseVector] = {
    val entries =
      records.keyBy(r => (r.maskedCustomerId, ETL.businessID(r))).mapValues(value => 1).reduceByKey(_ + _)
    val idx = entries.map(_._1._2).distinct.zipWithIndex.mapValues(_.toInt).collect().toMap

    entries.groupBy(_._1._1).mapValues(_.map(t => (t._1._2, t._2)))
    .mapValues(t => new SparseVector(idx.size, t.map(b => idx(b._1)).toArray, t.map(b => b._2.toDouble).toArray)).values
  }


}
