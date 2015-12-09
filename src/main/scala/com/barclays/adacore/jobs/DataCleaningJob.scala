package com.barclays.adacore.jobs

import com.barclays.adacore._
import com.barclays.adacore.utils.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

case object DataCleaningJob {
  def main(args: Array[String]) {
    val conf = new ScallopConf(args) with Serializable {
      val delim = opt[String](default = Some("\t"), descr = "The delimiter character")
      val anonymizedRecords = opt[String](required = false,
        descr = "The tables of the raw transactions data delimited by comma")
      val tmpFolder = opt[String](descr = "Overrides the directory used in spark.local.dir")
      val outputPath = opt[String](required = true, descr = "Output path of anonymized data")
      val minTransPerBusiness = opt[Int](required = false, default = Option(10), descr = "Minimum number of transactions per Businesses")
      val minTransPerUser = opt[Int](required = false, default = Option(5), descr = "Minimum number of transactions per User")
    }

    val sparkConf =
      new SparkConf().set("spark.akka.frameSize", "128").set("spark.hadoop.validateOutputSpecs", "false")
    if (conf.tmpFolder.isDefined) sparkConf.set("spark.local.dir", conf.tmpFolder())
    val sc: SparkContext = new SparkContext(sparkConf)

    conf.printHelp()
    Logger().info(conf.summary)

    val anonymizedRecords = sc.textFile(conf.anonymizedRecords()).map(AnonymizedRecord.fromSv())

    val badTowns =
      List("WESTON", "CREWKERNE", "KINGTON", "BAR", "BARWELL", "BAWDESWELL", "BELCOO", "BRIDGWATER", "BRIMSCOMBE",
        "BRIMSDOWN", "SOMERSET", "SOUTH GLOUCES", "SOUTHEND-ON-S", "STOKE ON TREN", "STOURBRIDGE", "STROUD", "THE MALL",
        "TAUNTON", "THATCHAM", "UK", "WALTON-ON-THA", "WEST REAGROUP", "WESTERN SUPER", "WESTMINSTER", "BRIXTON",
        "BURNHAM", "CAERPHILLY", "CARDIFF", "CHARD", "CHEDGRAVE", "CHIPPENHAM", "WINTERSTOKE", "WOLVERHAMPTON",
        "BRISTOL NAGS", "WSTN SPRMARE", "YEOVIL", "BRISTON", "BRIXTON MORLE", "LEOMINSTER", "LONDON", "MIDHURST",
        "NORTHWICH", "PLYMOUTH", "RADLETT", "RECEPTION", "REDFIELD", "RESTAURANT", "SHERBORNE", "DINGLES", "GATESHEAD",
        "GLASTONBURY", "GLENROTHES", "HACKBRIDGE", "KIDDERMINSTER"
      )

    val filteredRecords = anonymizedRecords.filter(t => !badTowns.exists(town => t.businessTown.contains(town)))

    val minTransPerUser = conf.minTransPerUser()
    val minTransPerBusiness = conf.minTransPerBusiness()

    val activeUsers: Set[Long] =
      filteredRecords.map(_.maskedCustomerId -> 1).reduceByKey(_ + _)
      .filter(_._2 > minTransPerUser).collect().map(_._1).toSet
    val activeUsersBV = sc.broadcast(activeUsers)

    val activeBusinesses: Set[(String, String)] =
      filteredRecords.map(_.businessKey -> 1)
      .reduceByKey(_ + _)
      .filter(_._2 > minTransPerBusiness)
      .collect().map(_._1).toSet
    val activeBusinessesBV = sc.broadcast(activeBusinesses)

    val filteredUserAmountBusiness = filteredRecords.filter(transaction =>
      activeUsersBV.value(transaction.maskedCustomerId) && activeBusinessesBV.value(transaction.businessKey)
    )

    Logger().info("records before cleaning: " + anonymizedRecords.count() +
      " records after cleaning: " + filteredUserAmountBusiness.count())

    filteredUserAmountBusiness.map(AnonymizedRecord.toSv()).saveAsTextFile(conf.outputPath())
  }
}
