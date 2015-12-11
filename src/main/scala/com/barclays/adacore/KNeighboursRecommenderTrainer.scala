package com.barclays.adacore

import com.barclays.adacore.utils.Pimps._
import com.barclays.adacore.utils.{Logger, TopElements, VPTree}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

case class KNeighboursCustomerFeatures(dayOfTheWeekWallet: Map[Int, Double],
                                       postcodeSectorWallet: Map[String, Double],
                                       categoryWallet: Map[String, Double],
                                       businessWallet: Map[(String, String), Double],
                                       onlineActivePdf: Map[Boolean, Double],
                                       acornTypeIdPdf: Map[Int, Double],
                                       genderPdf: Map[String, Double],
                                       maritalStatusIdPdf: Map[Int, Double],
                                       occupationIdPdf: Map[Int, Double])

case object KNeighboursRecommenderTrainer {
  def customerFeatures(@transient sc: SparkContext, data: RDD[AnonymizedRecord],
                       maxNumBusinessesPerNeighbour: Int): Array[(Long, KNeighboursCustomerFeatures)] = {

    Logger().info("Computing the category wallets...")
    val categoryWalletsBV: Broadcast[Map[Long, Map[String, Double]]] =
      sc.broadcast(wallet(data, _.merchantCategoryCode).collect().toMap)

    Logger().info("Computing the day of week wallets...")
    val dayOfTheWeekWalletsBV: Broadcast[Map[Long, Map[Int, Double]]] =
      sc.broadcast(wallet(data, _.dayOfWeek).collect().toMap)

    Logger().info("Computing the postcode wallets...")
    val postcodeSectorWalletsBV: Broadcast[Map[Long, Map[String, Double]]] =
      sc.broadcast(wallet(data.filter(_.businessPostcode.nonEmpty), _.businessPostcode.get.dropRight(2)).collect().toMap)

    Logger().info("Computing the business wallets...")
    val businessWalletsBV: Broadcast[Map[Long, Map[(String, String), Double]]] = sc.broadcast(
      wallet(data, _.businessKey)
      .mapValues(businessesScores =>
        TopElements.topN(businessesScores.toList)(_._2, maxNumBusinessesPerNeighbour).toMap
      ).collect().toMap
    )

    Logger().info("Computing the final customer features...")
    val customerFeatures: Array[(Long, KNeighboursCustomerFeatures)] =
      data.map(record => record.maskedCustomerId -> record.generalizedCategoricalGroup.group).distinct().map {
        case (customerId, categoricalGroup) =>
          customerId -> KNeighboursCustomerFeatures(dayOfTheWeekWallet = dayOfTheWeekWalletsBV.value(customerId),
            postcodeSectorWallet = postcodeSectorWalletsBV.value.getOrElse(customerId, Map.empty[String, Double]),
            categoryWallet = categoryWalletsBV.value(customerId),
            businessWallet = businessWalletsBV.value(customerId),
            onlineActivePdf = categoricalGroup.groupBy(_.onlineActive).mapValues(_.size).normalize.toList.toMap,
            acornTypeIdPdf = categoricalGroup.groupBy(_.acornTypeId).mapValues(_.size).normalize.toList.toMap,
            genderPdf = categoricalGroup.groupBy(_.gender).mapValues(_.size).normalize.toList.toMap,
            maritalStatusIdPdf =
              categoricalGroup.toList.flatMap(_.maritalStatusId).groupBy(identity).mapValues(_.size).normalize.toList.toMap,
            occupationIdPdf =
              categoricalGroup.toList.flatMap(_.occupationId).groupBy(identity).mapValues(_.size).normalize.toList.toMap)
      }.collect()

    postcodeSectorWalletsBV.destroy()
    categoryWalletsBV.destroy()
    dayOfTheWeekWalletsBV.destroy()
    businessWalletsBV.destroy()

    customerFeatures
  }

  def wallet[T](data: RDD[AnonymizedRecord], func: (AnonymizedRecord => T)): RDD[(Long, Map[T, Double])] =
    data.map(record => (record.maskedCustomerId, func(record)) -> 1).reduceByKey(_ + _)
    .groupBy {
      case ((customerId, _), _) => customerId
    }
    .mapValues(
      _.map {
        case ((_, t), count) => t -> count
      }
      .toMap.normalize.toList.toMap
    )

  def vpTree(customerFeatures: Array[(Long, KNeighboursCustomerFeatures)], minNumBusinessesPerCustomer: Int) = {
    Logger().info("Building the vptree with " + customerFeatures.length + " customers...")
    VPTree(customerFeatures.map(_.swap).filter(_._1.businessWallet.size > minNumBusinessesPerCustomer),
      distanceFunction, 1
    )
  }

  val distanceFunction: (KNeighboursCustomerFeatures, KNeighboursCustomerFeatures) => Double = {
    case ((customer1: KNeighboursCustomerFeatures), (customer2: KNeighboursCustomerFeatures)) =>
      val num = customer1.dayOfTheWeekWallet.productWithMap(customer2.dayOfTheWeekWallet) +
        customer1.postcodeSectorWallet.productWithMap(customer2.postcodeSectorWallet) +
        customer1.categoryWallet.productWithMap(customer2.categoryWallet) +
        customer1.businessWallet.productWithMap(customer2.businessWallet) +
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
}

case class KNeighboursRecommenderTrainer(@transient sc: SparkContext,
                                         minNumBusinessesPerCustomer: Int,
                                         maxNumBusinessesPerNeighbour: Int,
                                         maxPrecomputedRecommendations: Int,
                                         k: Int) extends RecommenderTrainer {
  def train(data: RDD[AnonymizedRecord]): Recommender = {
    //    val filteredCustomersAndBusinesses: RDD[(Long, (String, String))] =
    //      data.map(record => (record.maskedCustomerId, record.businessKey) -> 1).reduceByKey(_ + _)
    //      .filter(_._2 >= minNumTransactions).keys.cache()

    val customerFeatures = KNeighboursRecommenderTrainer.customerFeatures(sc, data, maxNumBusinessesPerNeighbour)

    val vpTree = KNeighboursRecommenderTrainer.vpTree(customerFeatures, minNumBusinessesPerCustomer)

    Logger().info("Pre-computing recommendations...")
    val preComputedRecommendationsBV = sc.broadcast(
      customerFeatures.par.map {
        case (customerId, features) => customerId -> {
          val businessesScores =
            (for {
              (neighbourFeatures, neighbourId) <- vpTree.approximateNearestN(features, k).par
              distance = KNeighboursRecommenderTrainer.distanceFunction(features, neighbourFeatures)
              (neighbourBusiness, neighbourFrequency) <- neighbourFeatures.businessWallet
              if !neighbourFeatures.businessWallet.contains(neighbourBusiness)
            } yield neighbourBusiness -> (neighbourFrequency * (1 - distance)))
            .groupBy(_._1).mapValues(_.map(_._2).sum).toList

          TopElements.topN(businessesScores)(_._2, maxPrecomputedRecommendations).map(_._1)
        }
      }
      .toList.toMap
    )

    Logger().info("Computing the most popular businesses...")
    val rankedBusinessesByNumberOfCustomersBV: Broadcast[List[(String, String)]] = sc.broadcast(
      mostPopularBusinesses(data).take(maxNumBusinessesPerNeighbour).toList
    )

    Logger().info("Creating the Recommender object...")
    new Recommender {
      // returns customerId -> List[(merchantName, merchantTown)]
      def recommendations(customers: RDD[Long], n: Int): RDD[(Long, List[(String, String)])] =
        customers.map(customerId => customerId -> {
          val recommendations =
            preComputedRecommendationsBV.value.getOrElse(customerId, rankedBusinessesByNumberOfCustomersBV.value)
            .take(n)
          if (recommendations.size >= n) recommendations
          else (recommendations ++
            (rankedBusinessesByNumberOfCustomersBV.value.take(n).toSet -- recommendations)).take(n)
        })
    }
  }
}
