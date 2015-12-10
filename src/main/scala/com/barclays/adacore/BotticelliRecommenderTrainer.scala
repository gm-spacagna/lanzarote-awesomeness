package com.barclays.adacore

import com.barclays.adacore.utils.Pimps._
import com.barclays.adacore.utils.{TopElements, VPTree}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scalaz.Scalaz._

case class BotticelliCustomerFeatures(dayOfTheWeekWallet: Map[Int, Double],
                                      postcodeSectorWallet: Map[String, Double],
                                      categoryWallet: Map[String, Double],
                                      onlineActivePdf: Map[Boolean, Double],
                                      acornTypeIdPdf: Map[Int, Double],
                                      genderPdf: Map[String, Double],
                                      maritalStatusIdPdf: Map[Option[Int], Double],
                                      occupationIdPdf: Map[Option[Int], Double])
case class BotticelliRecommenderTrainer(@transient sc: SparkContext, maxNumBusinessesPerNeighbour: Int, k: Int) extends RecommenderTrainer {
  def train(data: RDD[AnonymizedRecord]): Recommender = {

    def wallet[T](data: RDD[AnonymizedRecord], func: (AnonymizedRecord => T)): RDD[(Long, Map[T, Double])] =
      data.map(record => (record.maskedCustomerId, func(record)) -> 1).reduceByKey(_ + _)
      .map {
        case ((customerId, t), count) => customerId -> Map(t -> count)
      }
      .reduceByKey(_ |+| _)
      .mapValues(_.normalize)

    val businessWallets: Broadcast[Map[Long, Map[(String, String), Double]]] =
      sc.broadcast(
        wallet(data, _.businessKey)
        .mapValues(businessesScores =>
          TopElements.topN(businessesScores.toList)(_._2, maxNumBusinessesPerNeighbour).toMap
        ).collect().toMap
      )

    val categoryWallets: Broadcast[Map[Long, Map[String, Double]]] =
      sc.broadcast(wallet(data, _.merchantCategoryCode).collect().toMap)

    val dayOfTheWeekWallet: Broadcast[Map[Long, Map[Int, Double]]] =
      sc.broadcast(wallet(data, _.dayOfWeek).collect().toMap)

    val postcodeSectorWallet: Broadcast[Map[Long, Map[String, Double]]] =
      sc.broadcast(wallet(data.filter(_.businessPostcode.nonEmpty), _.businessPostcode.get.dropRight(2)).collect().toMap)

    val customerFeatures: Array[(Long, BotticelliCustomerFeatures)] =
      data.map(record => record.maskedCustomerId -> record.generalizedCategoricalGroup.group).distinct().map {
        case (customerId, categoricalGroup) =>
          customerId -> BotticelliCustomerFeatures(dayOfTheWeekWallet = dayOfTheWeekWallet.value(customerId),
            postcodeSectorWallet = postcodeSectorWallet.value(customerId),
            categoryWallet = categoryWallets.value(customerId),
            onlineActivePdf = categoricalGroup.groupBy(_.onlineActive).mapValues(_.size).normalize,
            acornTypeIdPdf = categoricalGroup.groupBy(_.acornTypeId).mapValues(_.size).normalize,
            genderPdf = categoricalGroup.groupBy(_.gender).mapValues(_.size).normalize,
            maritalStatusIdPdf = categoricalGroup.groupBy(_.maritalStatusId).mapValues(_.size).normalize,
            occupationIdPdf = categoricalGroup.groupBy(_.occupationId).mapValues(_.size).normalize)
      }.collect()

    val customerFeaturesBV = sc.broadcast(customerFeatures.toMap)

    val distanceFunction: (BotticelliCustomerFeatures, BotticelliCustomerFeatures) => Double = {
      case ((customer1: BotticelliCustomerFeatures), (customer2: BotticelliCustomerFeatures)) =>
        val num = customer1.dayOfTheWeekWallet.productWithMap(customer2.dayOfTheWeekWallet) +
          customer1.postcodeSectorWallet.productWithMap(customer2.postcodeSectorWallet) +
          customer1.categoryWallet.productWithMap(customer2.categoryWallet) +
          customer1.onlineActivePdf.productWithMap(customer2.onlineActivePdf) +
          customer1.acornTypeIdPdf.productWithMap(customer2.acornTypeIdPdf) +
          customer1.genderPdf.productWithMap(customer2.genderPdf) +
          customer1.maritalStatusIdPdf.productWithMap(customer2.maritalStatusIdPdf) +
          customer1.occupationIdPdf.productWithMap(customer2.occupationIdPdf)

        val den1 =
          math.sqrt(customer1.productIterator.flatMap(_.asInstanceOf[Map[_, Double]].values).map(math.pow(_, 2)).sum)

        val den2 =
          math.sqrt(customer2.productIterator.flatMap(_.asInstanceOf[Map[_, Double]].values).map(math.pow(_, 2)).sum)

        1 - (num / (den1 * den2))
    }

    val vpTree = VPTree(customerFeatures.map(_.swap), distanceFunction, 1)

    val vpTreeBV = sc.broadcast(vpTree)

    new Recommender {
      // returns customerId -> List[(merchantName, merchantTown)]
      def recommendations(customers: RDD[Long], n: Int): RDD[(Long, List[(String, String)])] =
        customers.map(customerId => customerId -> {
          val customerFeatures = customerFeaturesBV.value(customerId)
          val recommendable =
            (for {
              (neighbourFeatures, neighbourId) <- vpTreeBV.value.approximateNearestN(customerFeatures, k).toList
              distance = distanceFunction(customerFeatures, neighbourFeatures)
              (neighbourBusiness, neighbourFrequency) <- businessWallets.value(neighbourId)
            } yield neighbourBusiness -> (neighbourFrequency * distance))
            .groupBy(_._1).mapValues(_.map(_._2).sum).toList

          TopElements.topN(recommendable)(_._2, n).map(_._1)
        })
    }
  }
}
