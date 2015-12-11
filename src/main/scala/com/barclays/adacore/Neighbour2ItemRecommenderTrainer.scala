package com.barclays.adacore

import com.barclays.adacore.utils.{VPTree, Logger, TopElements}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

case class Neighbour2ItemRecommenderTrainer(@transient sc: SparkContext,
                                            minNumBusinessesPerCustomer: Int,
                                            maxNumBusinessesPerNeighbour: Int,
                                            maxPrecomputedRecommendations: Int,
                                            k: Int) extends RecommenderTrainer {
  def train(data: RDD[AnonymizedRecord]): Recommender = {
    val item2ItemMatrix: Map[(String, String), List[((String, String), Double)]] =
      Item2ItemRecommender.item2ItemMatrixLocal(businessKeyToCustomerIdSet(data).collect().toMap,
        Item2ItemRecommender.tanimotoSimilarity
      )

    val customerFeatures: Array[(Long, KNeighboursCustomerFeatures)] =
      KNeighboursRecommenderTrainer.customerFeatures(sc, data, maxNumBusinessesPerNeighbour)

    val vpTree: VPTree[KNeighboursCustomerFeatures, Long] =
      KNeighboursRecommenderTrainer.vpTree(customerFeatures, minNumBusinessesPerCustomer)

    Logger().info("Pre-computing recommendations...")
    val preComputedRecommendationsBV: Broadcast[Map[Long, List[(String, String)]]] = sc.broadcast(
      customerFeatures.par.map {
        case (customerId, features) => customerId -> {
          val neighbours = vpTree.approximateNearestN(features, k).par
          val similaritySum: Double =
            neighbours.map(_._1).map(KNeighboursRecommenderTrainer.distanceFunction(features, _)).map(1 - _).sum

          val businessWallet: Map[(String, String), Double] = features.businessWallet
          val businessesAndProbabilities: List[((String, String), Double)] =
            (for {
              (neighbourFeatures, neighbourId) <- neighbours
              distance = KNeighboursRecommenderTrainer.distanceFunction(features, neighbourFeatures)

              (neighbourBusiness, neighbourFrequency) <- neighbourFeatures.businessWallet
              if !neighbourFeatures.businessWallet.contains(neighbourBusiness)
            } yield neighbourBusiness -> (neighbourFrequency * (1 - distance) / similaritySum))
            .groupBy(_._1).mapValues(_.map(_._2).sum).toList ++ businessWallet

          val item2ItemBusinessesAndScores =
            (for {
              (businessKey, prob) <- businessesAndProbabilities
              similarityRow <- item2ItemMatrix.get(businessKey).toList
              rowSum = similarityRow.map(_._2).sum
              (similarBusiness, similarityScore) <- similarityRow
              if !businessWallet.contains(similarBusiness)
            } yield similarBusiness -> (similarityScore / rowSum * prob))
            .groupBy(_._1).mapValues(_.map(_._2).sum).toList

          TopElements.topN(item2ItemBusinessesAndScores)(_._2, maxPrecomputedRecommendations).map(_._1)
        }
      }
      .toList.toMap
    )

    Logger().info("Computing the most popular businesses...")
    val rankedBusinessesByNumberOfCustomersBV: Broadcast[List[(String, String)]] = sc.broadcast(
      mostPopularBusinesses(data).take(maxNumBusinessesPerNeighbour).toList
    )

    Logger().info("Creating the Recommender object...")
    new RecommenderWithPrecomputedRecommendationsAndMostPopularAsDefault(preComputedRecommendationsBV,
      rankedBusinessesByNumberOfCustomersBV)
  }
}
