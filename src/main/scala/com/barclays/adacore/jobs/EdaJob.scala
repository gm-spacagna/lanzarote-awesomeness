package com.barclays.adacore.jobs

import com.barclays.adacore.AnonymizedRecord
import com.vicpara.eda.{AppLogger}
import com.vicpara.eda.stats.{SequenceStats, PrettyPercentileStats}
import io.continuum.bokeh.{Document, GridPlot, Plot}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

case object EdaJob {
  def main(args: Array[String]) {
    val conf = new ScallopConf(args) {
      val rawPath = opt[String](required = true, descr = "The path of the raw business transactions")
      val output = opt[String](required = true, descr = "The path of the raw business transactions")
      val outputHuman = opt[String](required = true, descr = "The path of the raw business transactions")
    }

    val sc = Utils.staticSc
    val path = conf.rawPath()
    val transactions = sc.textFile(path = path)
                       .map(AnonymizedRecord.fromSv())
                       .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val results: RDD[PrettyPercentileStats] = sc.makeRDD(List(
      Stats.uniqueCustomers(transactions, 101),
      Stats.uniqueMerchants(transactions, 101),

      Stats.txCountPerBusiness(transactions, 1001),
      Stats.txCountPerCustomer(transactions, 1001),
      Stats.txCountPerMarital(transactions, 1001),
      Stats.txSumPerMarital(transactions, 1001)
    ).flatten,

      numSlices = 1)

    val out = conf.output()
    results.saveAsObjectFile(out)
    conf.outputHuman.foreach(results.map(_.toHumanReadable).saveAsTextFile)

    savePlotLists(results, "/tmp/eda/human/")
  }

  def savePlotLists(results: RDD[PrettyPercentileStats], outFolder: String) = {
    val plotGrid: List[List[Plot]] = results.collect()
                                     .flatMap(_.toPlot)
                                     .zipWithIndex
                                     .groupBy(_._2 / 2)
                                     .map(_._2.map(_._1).toList).toList

    val grid = new GridPlot().children(plotGrid)

    val document = new Document(grid)
    val html = document.save(outFolder + "/" + "EDA_Stats_Results.html")
    AppLogger.infoLevel.info(s"Wrote EDA stats charts in ${html.file}. Open ${html.url} in a web browser.")
  }
}

case object Stats {
  def businessID(tx: AnonymizedRecord) = tx.businessName + "_" + tx.businessTown + "_" + tx.businessPostcode
  def maritalToID(marital: Set[Option[Int]]) = marital.flatten.toList.sorted.mkString(",")

  def uniqueCustomers(tx: RDD[AnonymizedRecord], nPercentiles: Int = 1001) =
    Some("Unique Customers")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.distinct[AnonymizedRecord, Long, Long](
        data = AppLogger.logStage(tx, n),
        toDrillDownKeyOption = None,
        toDimKey = tx => 1l,
        toVal = tx => tx.maskedCustomerId,
        numPercentiles = nPercentiles
      )
    ))

  def uniqueMerchants(tx: RDD[AnonymizedRecord], nPercentiles: Int = 1001) =
    Some("Unique Customers")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.distinct[AnonymizedRecord, Long, String](
        data = AppLogger.logStage(tx, n),
        toDrillDownKeyOption = None,
        toDimKey = tx => 1l,
        toVal = tx => businessID(tx),
        numPercentiles = nPercentiles
      )
    ))

  def txCountPerBusiness(tx: RDD[AnonymizedRecord], nPercentiles: Int = 1001) =
    Some("BusinessID - Count(Tx)")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.percentile[AnonymizedRecord, String, Long](
        data = AppLogger.logStage(tx, n),
        toDrillDownKeyOption = None,
        toDimKey = tx => businessID(tx),
        toVal = tx => 1l,
        reduceFunc = _ + _,
        toStats = identity,
        numPercentiles = nPercentiles
      )
    ))

  def txCountPerCustomer(tx: RDD[AnonymizedRecord], nPercentiles: Int = 1001) =
    Some("CustomerID - Count(Tx)")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.percentile[AnonymizedRecord, Long, Long](
        data = AppLogger.logStage(tx, n),
        toDrillDownKeyOption = None,
        toDimKey = tx => tx.maskedCustomerId,
        toVal = tx => 1l,
        reduceFunc = _ + _,
        toStats = identity,
        numPercentiles = nPercentiles
      )
    ))

  def txCountPerMarital(tx: RDD[AnonymizedRecord], nPercentiles: Int = 1001) =
    Some("MaritalStatus - Count(Tx)")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.percentile[AnonymizedRecord, String, Long](
        data = AppLogger.logStage(tx, n),
        toDrillDownKeyOption = None,
        toDimKey = tx => maritalToID(tx.maritalStatusId),
        toVal = tx => 1l,
        reduceFunc = _ + _,
        toStats = identity,
        numPercentiles = nPercentiles
      )
    ))

  def txSumPerMarital(tx: RDD[AnonymizedRecord], nPercentiles: Int = 1001) =
    Some("MaritalStatus - Sum(Tx)")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.percentile[AnonymizedRecord, String, Double](
        data = AppLogger.logStage(tx, n),
        toDrillDownKeyOption = None,
        toDimKey = tx => maritalToID(tx.maritalStatusId),
        toVal = tx => tx.amount,
        reduceFunc = _ + _,
        toStats = t => t.toLong,
        numPercentiles = nPercentiles
      )
    ))
}

case object Utils {
  lazy val staticSc = new SparkContext(
    new SparkConf().setMaster("local")
    .set("spark.ui.port", "14311")
    .set("spark.eventLog.dir", "/tmp/spark-temp/")
    .set("spark.io.compression.codec", "lz4")
    .setAppName("Test Local Spark Context"))
}
