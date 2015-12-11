package com.barclays.adacore

import com.barclays.adacore.utils.TopElements
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{Vectors, Vector, SparseVector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._
import org.apache.spark.storage.StorageLevel

import scalaz.Scalaz._

case class RandomForestRecommender(@transient sc: SparkContext, minNumTransactions: Int) extends RecommenderTrainer {
  def businessID(tx: AnonymizedRecord) = (tx.businessName, tx.businessTown)

  def features(records: RDD[AnonymizedRecord]): (RDD[(Long, Vector)], Broadcast[Map[(String, String), Int]]) = {
    val entries: RDD[((Long, (String, String)), (Int, Double))] =
      records.keyBy(r => (r.maskedCustomerId, businessID(r)))

      .mapValues(value => (1, value.amount))
      .reduceByKey(_ |+| _)

    val businessIdx: Broadcast[Map[(String, String), Int]] =
      entries.context.broadcast(entries.map(_._1._2).distinct.zipWithIndex.mapValues(_.toInt).collect().toMap)

    (entries.keyBy(_._1._1).mapValues(v => List((v._1._2, v._2)))
     .reduceByKey(_ |+| _)
     .mapPartitions(part => {
       val busIdxBV = businessIdx.value

       part.map(e => {
         val (cId, col) = e
         val sv = Vectors.sparse(busIdxBV.size, col.map(el => busIdxBV(el._1) -> el._2._1.toDouble))
         (cId, sv)
       })
     }), businessIdx)
  }

  def train(data: RDD[AnonymizedRecord]): Recommender = {

    val (trainingData, businessIdx) = features(data)

    trainingData.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val entries: RDD[((Long, (String, String)), (Int, Double))] =
      data.keyBy(r => (r.maskedCustomerId, businessID(r)))
      .mapValues(value => (1, value.amount))
      .reduceByKey(_ |+| _)

    val models =
      for{
        (business, id) <- businessIdx.value.par
        labels = trainingData.map(v => LabeledPoint(Math.min(v._2.apply(id), 1d) , v._2))
        model = RandomForest.trainClassifier(labels, 2, Map[Int,Int](), 20, "auto", "entropy", 4, 24)
      } yield (business, model)

    val filteredCustomersAndBusinesses: RDD[(Long, (String, String))] =
      data.map(record => (record.maskedCustomerId, record.businessKey) -> 1).reduceByKey(_ + _)
      .filter(_._2 >= minNumTransactions).keys.cache()

    val customerIdToBusinessSet: Broadcast[Map[Long, Set[(String, String)]]] =
      sc.broadcast(filteredCustomersAndBusinesses.mapValues(Set(_)).reduceByKey(_ ++ _).collect().toMap)

    val businessKeyToCustomerSet: RDD[((String, String), Set[Long])] =
      filteredCustomersAndBusinesses.map(_.swap).mapValues(Set(_)).reduceByKey(_ ++ _).cache()

    val rankedBusinessesByNumberOfCustomers = sc.broadcast(
      businessKeyToCustomerSet.mapValues(_.size.toDouble).sortBy(_._2, ascending = false).collect().toList
    )

    new Recommender {
      // returns customerId -> List[(merchantName, merchantTown)]
      def recommendations(customers: RDD[Long], n: Int): RDD[(Long, List[(String, String)])] =
        customers.keyBy(identity).join(trainingData).mapValues(_._2).mapValues(vector =>
          (for {
              model <- models
              if vector.apply(businessIdx.value(model._1)) == 0d
              pred = model._2.predict(vector)
            } yield model._1 -> pred).toList |> { businessesScores =>
              val recommendations = TopElements.topN(businessesScores)(_._2, n).map(_._1)
              if (recommendations.size >= n) recommendations
              else (recommendations ++
                (rankedBusinessesByNumberOfCustomers.value.take(n).map(_._1).toSet -- recommendations)).take(n)
            })
    }

  }
}
