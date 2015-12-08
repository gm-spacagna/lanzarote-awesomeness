package com.barclays.adacore.jobs

import com.barclays.adacore._
import com.barclays.adacore.utils.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

case object DataCleaningJob {
  def main(args: Array[String]) {
    val conf = new ScallopConf(args) {
      val delim = opt[String](default = Some("\t"), descr = "The delimiter character")
      val anonymizedRecords = opt[String](required = false,
        descr = "The tables of the raw transactions data delimited by comma")
      val tmpFolder = opt[String](descr = "Overrides the directory used in spark.local.dir")
      val outputPath = opt[String](required = true, descr = "Output path of anonymized data")
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

    val filteredRecords = anonymizedRecords.filter(t => badTowns.filter(town => t.businessTown.contains(town)).size == 0)

    filteredRecords.map(AnonymizedRecord.toSv()).saveAsTextFile(conf.outputPath())

  }
}
