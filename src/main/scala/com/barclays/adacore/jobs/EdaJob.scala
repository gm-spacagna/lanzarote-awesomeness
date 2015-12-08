package com.barclays.adacore.jobs

import com.barclays.adacore.AnonymizedRecord
import com.vicpara.eda.{AppLogger}
import com.vicpara.eda.stats.{SequenceStats, PrettyPercentileStats}
import io.continuum.bokeh.{Document, GridPlot, Plot}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

import scalaz.Scalaz._

case object EdaJob {
  def main(args: Array[String]) {
    val conf = new ScallopConf(args) {
      val rawPath = opt[String](required = true, descr = "The path of the raw business transactions")
      val output = opt[String](required = true, descr = "The path of the raw business transactions")
      val outputHuman = opt[String](required = true, descr = "The path of the raw business transactions")
    }

    val sc = Utils.staticSc
    val inPath = conf.rawPath()
    val transactions = sc.textFile(path = inPath)
                       .map(AnonymizedRecord.fromSv())
                       .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val results: RDD[PrettyPercentileStats] = sc.makeRDD(List(
      //      Stats.uniqueCustomers(transactions, 101),
      //      Stats.uniqueMerchants(transactions, 101),
      //
      //      Stats.txCountPerBusiness(transactions, 1001),
      //      Stats.txCountPerCustomer(transactions, 1001),
      //      Stats.txCountPerMarital(transactions, 1001),
      //      Stats.txSumPerMarital(transactions, 1001),
      //
      Stats.txSumPerDayOfWeek(transactions, 1001),
      Stats.txAmountPerCustomer(transactions, 1001),
      Stats.customerCountPerTxAmount(transactions, 1001),
      Stats.averageTxAmountPerBusiness(transactions, 1001),
      Stats.averageTxAmountPerCustomer(transactions, 1001),
      Stats.countTxPerDistrict(transactions, 1001),
      Stats.sumTxAmountPerDistrict(transactions, 1001),
      Stats.averageTxAmountPerDistrict(transactions, 1001),
      Stats.uniqueMerchantCodestPerDistrict(transactions, 1001),
      Stats.averageTxAmountPerMerchantCode(transactions, 1001),
      Stats.averageTxAmountPerMerchantCodeAndGender(transactions, 1001)

    ).flatten,

      numSlices = 1)

    val out = conf.output()
    results.saveAsObjectFile(out)
    conf.outputHuman.foreach(results.map(_.toHumanReadable).saveAsTextFile)

    savePlotLists(results, conf.outputHuman())
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
  def toDistrictCode(postcode: Option[String]): String = postcode match {
    case Some(a) if a.nonEmpty => a.substring(0, a.length - 3)
    case _ => "$NONE_POSTCODE$"
  }

  def genderVal(gender: Set[String]): String = gender match {
    case a if a.nonEmpty => a.head
    case _ => "$NONE_GENDER$"
  }

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

  def txSumPerDayOfWeek(tx: RDD[AnonymizedRecord], nPercentiles: Int = 1001) =
    Some("DayOfWeek - Sum(Tx)")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.percentile[AnonymizedRecord, Int, Double](
        data = AppLogger.logStage(tx, n),
        toDrillDownKeyOption = None,
        toDimKey = tx => tx.dayOfWeek,
        toVal = tx => tx.amount,
        reduceFunc = _ + _,
        toStats = t => t.toLong,
        numPercentiles = nPercentiles
      )
    ))

  def txAmountPerCustomer(tx: RDD[AnonymizedRecord], nPercentiles: Int = 1001) =
    Some("Customer - Sum(Tx)")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.percentile[AnonymizedRecord, Long, Double](
        data = AppLogger.logStage(tx, n),
        toDrillDownKeyOption = None,
        toDimKey = tx => tx.maskedCustomerId,
        toVal = tx => tx.amount,
        reduceFunc = _ + _,
        toStats = t => t.round,
        numPercentiles = nPercentiles
      )
    ))

  def customerCountPerTxAmount(tx: RDD[AnonymizedRecord], nPercentiles: Int = 1001) =
    Some("TxAmount - Count of Customers")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.percentile[AnonymizedRecord, Long, Set[Long]](
        data = AppLogger.logStage(tx, n),
        toDrillDownKeyOption = None,
        toDimKey = tx => tx.amount.round,
        toVal = tx => Set(tx.maskedCustomerId),
        reduceFunc = _ ++ _,
        toStats = t => t.size,
        numPercentiles = nPercentiles
      )
    ))

  def averageTxAmountPerBusiness(tx: RDD[AnonymizedRecord], nPercentiles: Int = 1001) =
    Some("Business - Avg(TxAmount)")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.percentile[AnonymizedRecord, String, (Double, Int)](
        data = AppLogger.logStage(tx, n),
        toDrillDownKeyOption = None,
        toDimKey = tx => businessID(tx),
        toVal = tx => (tx.amount, 1),
        reduceFunc = _ |+| _,
        toStats = stats => (stats._1 / stats._2).round,
        numPercentiles = nPercentiles
      )
    ))

  def averageTxAmountPerCustomer(tx: RDD[AnonymizedRecord], nPercentiles: Int = 1001) =
    Some("Customer - Avg(TxAmount)")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.percentile[AnonymizedRecord, Long, (Double, Int)](
        data = AppLogger.logStage(tx, n),
        toDrillDownKeyOption = None,
        toDimKey = tx => tx.maskedCustomerId,
        toVal = tx => (tx.amount, 1),
        reduceFunc = _ |+| _,
        toStats = stats => (stats._1 / stats._2).round,
        numPercentiles = nPercentiles
      )
    ))

  def countTxPerDistrict(tx: RDD[AnonymizedRecord], nPercentiles: Int = 1001) =
    Some("District - Count(Tx)")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.percentile[AnonymizedRecord, String, Long](
        data = AppLogger.logStage(tx, n),
        toDrillDownKeyOption = None,
        toDimKey = tx => toDistrictCode(tx.businessPostcode),
        toVal = tx => 1l,
        reduceFunc = _ |+| _,
        toStats = identity,
        numPercentiles = nPercentiles
      )
    ))

  def sumTxAmountPerDistrict(tx: RDD[AnonymizedRecord], nPercentiles: Int = 1001) =
    Some("District - Sum(Tx)")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.percentile[AnonymizedRecord, String, Double](
        data = AppLogger.logStage(tx, n),
        toDrillDownKeyOption = None,
        toDimKey = tx => toDistrictCode(tx.businessPostcode),
        toVal = tx => tx.amount,
        reduceFunc = _ |+| _,
        toStats = s => s.round,
        numPercentiles = nPercentiles
      )
    ))

  def averageTxAmountPerDistrict(tx: RDD[AnonymizedRecord], nPercentiles: Int = 1001) =
    Some("District - Avg(TxAmount)")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.percentile[AnonymizedRecord, String, (Double, Int)](
        data = AppLogger.logStage(tx, n),
        toDrillDownKeyOption = None,
        toDimKey = tx => toDistrictCode(tx.businessPostcode),
        toVal = tx => (tx.amount, 1),
        reduceFunc = _ |+| _,
        toStats = stats => (stats._1 / stats._2).round,
        numPercentiles = nPercentiles
      )
    ))

  def uniqueMerchantCodestPerDistrict(tx: RDD[AnonymizedRecord], nPercentiles: Int = 1001) =
    Some("District - Unique Merchant Codes")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.percentile[AnonymizedRecord, String, Set[String]](
        data = AppLogger.logStage(tx, n),
        toDrillDownKeyOption = None,
        toDimKey = tx => toDistrictCode(tx.businessPostcode),
        toVal = tx => Set(tx.merchantCategoryCode),
        reduceFunc = _ |+| _,
        toStats = stats => stats.size,
        numPercentiles = nPercentiles
      )
    ))

  //  Gender merchant code - average transaciton count
  def averageTxAmountPerMerchantCode(tx: RDD[AnonymizedRecord], nPercentiles: Int = 1001) =
    Some("Merchant Code - Average Transaction Value")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.percentile[AnonymizedRecord, String, (Double, Int)](
        data = AppLogger.logStage(tx, n),
        toDrillDownKeyOption = None,
        toDimKey = tx => tx.merchantCategoryCode,
        toVal = tx => (tx.amount, 1),
        reduceFunc = _ |+| _,
        toStats = stats => (stats._1 / stats._2).round,
        numPercentiles = nPercentiles
      )
    ))

  def averageTxAmountPerMerchantCodeAndGender(tx: RDD[AnonymizedRecord], nPercentiles: Int = 1001) =
    Some("Gender x Merchant Code - Average Transaction Value")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.percentile[AnonymizedRecord, (String, String), (Double, Int)](
        data = AppLogger.logStage(tx, n),
        toDrillDownKeyOption = None,
        toDimKey = tx => (tx.merchantCategoryCode, genderVal(tx.gender)),
        toVal = tx => (tx.amount, 1),
        reduceFunc = _ |+| _,
        toStats = stats => (stats._1 / stats._2).round,
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
