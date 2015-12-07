package com.barclays.adacore.jobs

import com.barclays.adacore._
import com.barclays.adacore.anonymizers.{AccountAnonymizer, GeneralizedCategoricalBucketGroup}
import com.barclays.adacore.utils.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.rogach.scallop.ScallopConf

case object BristolETLJob {
  def main(args: Array[String]) {
    val conf = new ScallopConf(args) {
      val delim = opt[String](default = Some("\t"), descr = "The delimiter character")
      val rawTransactionsTables = opt[String](required = false,
        descr = "The tables of the raw transactions data delimited by comma")
      val rawBusinessPath = opt[String](required = true, descr = "The path of the raw business data")
      val tmpFolder = opt[String](descr = "Overrides the directory used in spark.local.dir")
      val teradataUsername = opt[String](required = true, descr = "The username for teradata")
      val teradataPassword = opt[String](required = true, descr = "The password for teradata")
      val minCustomerId = opt[Long](default = Some(1517945l), descr = "minimum value of customer id in the tables")
      val maxCustomerId = opt[Long](default = Some(9996927217l), descr = "maximum value of customer id in the tables")
      val k = opt[Int](default = Some(10), descr = "minimum number of records per bucket in k-anonymity")
      val outputPath = opt[String](required = true, descr = "Output path of anonymized data")
    }

    val sparkConf =
      new SparkConf().set("spark.akka.frameSize", "128").set("spark.hadoop.validateOutputSpecs", "false")
    if (conf.tmpFolder.isDefined) sparkConf.set("spark.local.dir", conf.tmpFolder())
    val sc: SparkContext = new SparkContext(sparkConf)

    conf.printHelp()
    Logger().info(conf.summary)

    val tables = Tables(conf.teradataUsername(), new Password(conf.teradataPassword()), new SQLContext(sc))

    val rawTransactions: RDD[RawTransactionWithoutTime] =
      conf.rawTransactionsTables().split(",")
      .map(tables.rawTransactionsWithoutTime(_, conf.minCustomerId(), conf.maxCustomerId()))
      .reduce(_ ++ _)
      .cache()


    Logger().info("transactions loaded in total are " + rawTransactions.count())

    val rawCustomers: RDD[RawCustomer] =
      tables.rawCustomers().keyBy(_._2.customerId).reduceByKey {
        case (left@(dateLeft, accountLeft), right@(dateRight, accountRight)) =>
          if (dateLeft > dateRight) left else right
      }
      .values.values.cache()

    Logger().info("reduced to " + rawCustomers.count() + " distinct raw customers")

    val rawBusinesses: RDD[RawBusiness] = sc.textFile(conf.rawBusinessPath()).map(RawBusiness(conf.delim())).cache()

    Logger().info("loaded " + rawBusinesses.count() + " raw businesses")

    val maskingMapBV: Broadcast[Map[Long, Long]] =
      sc.broadcast(rawTransactions.map(_.customerId).distinct().zipWithUniqueId().collect().toMap)

    val generalizedCustomers: RDD[(Long, GeneralizedCategoricalBucketGroup)] =
      AccountAnonymizer.generalizeByCategoricalBuckets(sc, rawCustomers, conf.k()).flatMap {
        case (generalizedGroup, customers) => customers.map(_.customerId -> generalizedGroup)
      }
      .cache()

    Logger().info("generalized raw customers to " + generalizedCustomers.map(_._2).distinct().count() +
      " categorical bucket groups of at least " + conf.k() + " records each")

    Logger().info("Joining transactions, customers and businesses into a single fat anonymized table")

    val transactionsAndCustomerInfo =
      rawTransactions.keyBy(_.customerId).join(generalizedCustomers).map {
        case (customerId, (transaction, bucketGroup)) =>
          (transaction.merchantName.trim.toUpperCase, transaction.merchantTown.trim.toUpperCase) ->
            (maskingMapBV.value(customerId), transaction, bucketGroup)
      }
      .cache()

    Logger().info("records after joining transactions and customers" + transactionsAndCustomerInfo.count())

    val anonymizedRecords: RDD[AnonymizedRecord] =
      transactionsAndCustomerInfo
      .leftOuterJoin(rawBusinesses.keyBy(b => (b.merchantName, b.merchantTown))).map {
        case ((merchantName, merchantTown), ((maskedCustomerId, transaction, bucketGroup), business)) =>
          AnonymizedRecord(maskedCustomerId, bucketGroup, transaction.amount,
            DateTime.parse(transaction.date).dayOfWeek().get,
            transaction.merchantCategoryCode, business.map(_.name).getOrElse(merchantName),
            business.map(_.town).getOrElse(merchantTown), business.map(_.postcode))
      }
      .cache()

    anonymizedRecords.map(AnonymizedRecord.toSv(sep = conf.delim())).saveAsTextFile(conf.outputPath())

    Logger().info("joined transactions, customers and businesses into a single fat anonymized data stored at " +
      conf.outputPath() + " size is " + anonymizedRecords.count())
  }
}
