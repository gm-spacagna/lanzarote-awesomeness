package com.barclays.adacore.jobs

import com.barclays.adacore.utils.Logger
import com.barclays.adacore.{ALSRecommender, AnonymizedRecord, RecommenderEvaluation}
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

case object ALSEvaluationJob {
  def main(args: Array[String]) {
    val conf = new ScallopConf(args) {
      val anonymizedDataPath = opt[String](required = true, descr = "The path of the anonymized data")
      val tmpFolder = opt[String](descr = "Overrides the directory used in spark.local.dir")
      val n = opt[Int](default = Some(20), descr = "the number of recommendation to evaluate")
      val partitions = opt[Int](default = Some(100), descr = "the number of partitions to coalesce to")
      val persist = toggle(default = Some(false), descrYes = "persist data to disk and memory serialized")
      val trainingFraction = opt[Double](default = Some(0.8),
        descr = "the fraction of businesses to use in the training for each customer")

      val rank = opt[Int](default = Some(10))
      val numIterations = opt[Int](default = Some(10))
      val alpha = opt[Double](default = Some(0.01))
      val blocks = opt[Int](default = Some(30))
      val lambda = opt[Double](default = Some(0.01))

      val sampleFraction = opt[Double](default = Some(0.01))
    }

    val sparkConf =
      new SparkConf().set("spark.akka.frameSize", "128").set("spark.hadoop.validateOutputSpecs", "false")
    if (conf.tmpFolder.isDefined) sparkConf.set("spark.local.dir", conf.tmpFolder())
    val sc: SparkContext = new SparkContext(sparkConf)

    conf.printHelp()
    Logger().info(conf.summary)

    val data = sc.textFile(conf.anonymizedDataPath()).coalesce(conf.partitions()).map(AnonymizedRecord.fromSv())
    val recommender =
      ALSRecommender(sc, conf.rank(), conf.numIterations(), conf.alpha(), conf.blocks(), conf.lambda(), conf.n(),
        conf.sampleFraction())

    val (trainingData, testData) = RecommenderEvaluation(sc).splitData(data, conf.trainingFraction())

    Logger().info("MAP@" + conf.n() + "=" + RecommenderEvaluation(sc).evaluate(recommender, trainingData, testData, conf.n()))
  }
}
