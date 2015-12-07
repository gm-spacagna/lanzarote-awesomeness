package com.barclays.adacore.utils

import org.apache.spark.{SparkConf, SparkContext}

object StaticSparkContext {
  val staticSc = new SparkContext(
    new SparkConf().setMaster("local")
    .set("spark.ui.port","14311")
    .set("spark.eventLog.dir","/tmp/spark-temp/")
    .set("spark.io.compression.codec","lz4")
    .setAppName("Test Local Spark Context")
  )
}

trait TestUtils {
  @transient lazy val sc = StaticSparkContext.staticSc
}
