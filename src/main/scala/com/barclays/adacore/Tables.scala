package com.barclays.adacore

import com.barclays.adacore.utils.Pimps._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scalaz.Scalaz._

case object Password {
  val PasswordTag = "<PASSWORD>"
}

class Password(pswd: String) {
  def replaceInString(str: String) = str.replaceAll(Password.PasswordTag, pswd)
}

case object Tables {
  val Url = "jdbc:teradata://dwsana.dws.barclays.co.uk/"
  val Driver = "com.teradata.jdbc.TeraDriver"

  def load(sqlctx: SQLContext, url: String = Url, driver: String = Driver, username: String, password: Password)
          (database: String, table: String, partitionColumn: String, lowerBound: Long, upperBound: Long,
           numPartitions: Int) =
    sqlctx.load(
      source = "jdbc",
      options = Map("url" -> password.replaceInString(Url +
        s"DATABASE=$database,USER=$username,PASSWORD=" + Password.PasswordTag
      ), "dbtable" -> table,
        "driver" -> driver,
        "partitionColumn" -> partitionColumn,
        "lowerBound" -> lowerBound.toString,
        "upperBound" -> upperBound.toString,
        "numPartitions" -> numPartitions.toString
      )
    )
}

case class Tables(username: String, password: Password, @transient sqlctx: SQLContext) {
  val load = Tables.load(sqlctx = sqlctx, username = username, password = password) _

  def rawAccountRDD(): RDD[(String, RawAccount)] =
    load("D_LB_PRODUCT", "MVP_CTM", "CUSTOMER_IDENTIFIER", 1510072, 9996928884l, 12)
    .select("SUMMARY_DATE", "CUSTOMER_IDENTIFIER", "CA_AVG_CLEARED_BALANCE_AMT", "AGE", "ONLINE_ACTIVE", "CUSTOMER_INCOME",
        "GROSS_INCOME_AMOUNT", "POSTAL_SECTOR", "ACORN_TYPE_ID", "SEX_INDICATOR", "MARITAL_STATUS_CODE",
        "OCCUPATION_CODE")
    .map(_.toSeq.toList match {
      case List(summaryDate: java.sql.Date, customerId: java.math.BigDecimal, balance, age, online: Int,
      income, grossIncome: Int, postalSector, acornTypeId: Int, gender: String,
      maritalStatusId: String, occupationId: String) => summaryDate.toString -> RawAccount(customerId.longValue(),
        Option(balance.asInstanceOf[java.math.BigDecimal]).map(_.doubleValue), Option(age.asInstanceOf[Int]),
        online == 1, Option(income.asInstanceOf[java.math.BigDecimal]).map(_.doubleValue), grossIncome,
        Option(postalSector.asInstanceOf[String]),
        acornTypeId, gender, maritalStatusId.trim.nonEmptyOption(_.toInt),
        occupationId.forall((char: Char) => char.isDigit).option(occupationId.toInt))
    })
}
