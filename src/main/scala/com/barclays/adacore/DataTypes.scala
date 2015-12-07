package com.barclays.adacore

import com.barclays.adacore.anonymizers.{CategoricalBucket, GeneralizedCategoricalBucketGroup}
import com.barclays.adacore.utils.Pimps._

case class RawCustomer(customerId: Long, balance: Option[Double], age: Option[Int], onlineActive: Boolean,
                       income: Option[Double], grossIncome: Double, postalSector: Option[String], acornTypeId: Int,
                       gender: String, maritalStatusId: Option[Int], occupationId: Option[Int])

case class RawTransactionWithoutTime(merchantName: String, merchantTown: String,
                                     merchantCountry: String, merchantCategoryCode: String, amount: Double,
                                     date: String, customerId: Long)

case object RawBusiness {
  def apply(sep: String = "\t")(line: String): RawBusiness = line.split(sep, -1).toList match {
    case List(merchantName, merchantTown, name, town, postcode) =>
      RawBusiness(merchantName, merchantTown, name, town, postcode)
  }
}

case class RawBusiness(merchantName: String, merchantTown: String, name: String, town: String, postcode: String)

case object AnonymizedRecord {
  def toSv(sep: String = "\t", listSep: String = ",", categoricalBucketSep: String = "|")(record: AnonymizedRecord): String = {
    def categoryBucketToSV(bucket: CategoricalBucket): String = bucket match {
      case CategoricalBucket(onlineActive, acornTypeId, gender, maritalStatusId, occupationId) =>
        List(onlineActive.toString, acornTypeId, gender, maritalStatusId.getOrElse(""), occupationId.getOrElse(""))
        .mkString(categoricalBucketSep)
    }
    import record._

    List(maskedCustomerId, generalizedCategoricalGroup.group.map(categoryBucketToSV).mkString(listSep),
      generalizedCategoricalGroup.groupSize, amount, dayOfWeek, merchantCategoryCode, businessName, businessTown,
      businessPostcode.getOrElse("")).mkString(sep)
  }

  def fromSv(sep: String = "\t", listSep: String = ",", categoricalBucketSep: String = "|")(line: String): AnonymizedRecord =
    line.split(sep, -1).toList match {
      case List(maskedCustomerId, generalizedCategoricalGroup,
      groupSize, amount, dayOfWeek, merchantCategoryCode, businessName, businessTown, businessPostcode) =>

        def svToCategoryBucket(categoryBucketString: String): CategoricalBucket =
          categoryBucketString.split(categoricalBucketSep, -1).toList match {
            case List(onlineActive, acornTypeId, gender, maritalStatusId, occupationId) =>
              CategoricalBucket(onlineActive == "true", acornTypeId.toInt, gender, maritalStatusId.nonEmptyOption(_.toInt),
                occupationId.nonEmptyOption(_.toInt))
          }

        AnonymizedRecord(maskedCustomerId.toLong,
          GeneralizedCategoricalBucketGroup(
            generalizedCategoricalGroup.split(listSep).map(svToCategoryBucket).toSet, groupSize.toInt
          ),
          amount.toDouble, dayOfWeek.toInt, merchantCategoryCode, businessName, businessTown,
          businessPostcode.nonEmptyOption
        )
    }
}

case class AnonymizedRecord(maskedCustomerId: Long, generalizedCategoricalGroup: GeneralizedCategoricalBucketGroup,
                            amount: Double, dayOfWeek: Int, merchantCategoryCode: String,
                            businessName: String, businessTown: String, businessPostcode: Option[String])
