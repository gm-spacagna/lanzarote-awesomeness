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

  def fromSv(sep: String = "\t", listSep: String = ",", categoricalBucketSep: String = """\|""")(line: String): AnonymizedRecord =
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
                            businessName: String, businessTown: String, businessPostcode: Option[String]) {
  def onlineActive: Set[Boolean] = generalizedCategoricalGroup.group.map(_.onlineActive)

  def acornTypeId: Set[Int] = generalizedCategoricalGroup.group.map(_.acornTypeId)

  def gender: Set[String] = generalizedCategoricalGroup.group.map(_.gender)

  def maritalStatusId: Set[Option[Int]] = generalizedCategoricalGroup.group.map(_.maritalStatusId)

  def businessKey: (String, String) = (businessName, businessTown)

  def occupationId: Set[Option[Int]] = generalizedCategoricalGroup.group.map(_.occupationId)
}

case object CustomerToBusinessStatistics {
  def toSv(sep: String = "\t", listSep: String = ",", categoricalBucketSep: String = "|")(statistic: CustomerToBusinessStatistics): String = {
    def categoryBucketToSV(bucket: CategoricalBucket): String = bucket match {
      case CategoricalBucket(onlineActive, acornTypeId, gender, maritalStatusId, occupationId) =>
        List(onlineActive.toString, acornTypeId, gender, maritalStatusId.getOrElse(""), occupationId.getOrElse(""))
        .mkString(categoricalBucketSep)
    }
    import statistic._

    List(maskedCustomerId, generalizedCategoricalGroup.group.map(categoryBucketToSV).mkString(listSep),
      generalizedCategoricalGroup.groupSize, amountSum, visits, merchantCategoryCode, businessName, businessTown,
      businessPostcode.getOrElse("")).mkString(sep)
  }

  def fromSv(sep: String = "\t", listSep: String = ",", categoricalBucketSep: String = """\|""")(line: String): CustomerToBusinessStatistics =
    line.split(sep, -1).toList match {
      case List(maskedCustomerId, generalizedCategoricalGroup,
      groupSize, amountSum, visits, merchantCategoryCode, businessName, businessTown, businessPostcode) =>

        def svToCategoryBucket(categoryBucketString: String): CategoricalBucket =
          categoryBucketString.split(categoricalBucketSep, -1).toList match {
            case List(onlineActive, acornTypeId, gender, maritalStatusId, occupationId) =>
              CategoricalBucket(onlineActive == "true", acornTypeId.toInt, gender, maritalStatusId.nonEmptyOption(_.toInt),
                occupationId.nonEmptyOption(_.toInt))
          }

        CustomerToBusinessStatistics(maskedCustomerId.toLong,
          GeneralizedCategoricalBucketGroup(
            generalizedCategoricalGroup.split(listSep).map(svToCategoryBucket).toSet, groupSize.toInt
          ),
          amountSum.toDouble, visits.toInt, merchantCategoryCode, businessName, businessTown,
          businessPostcode.nonEmptyOption
        )
    }
}





case class CustomerToBusinessStatistics(maskedCustomerId: Long,
                                        generalizedCategoricalGroup: GeneralizedCategoricalBucketGroup,
                                        amountSum: Double, visits: Int, merchantCategoryCode: String,
                                        businessName: String, businessTown: String, businessPostcode: Option[String]) {
  def onlineActive: Set[Boolean] = generalizedCategoricalGroup.group.map(_.onlineActive)

  def acornTypeId: Set[Int] = generalizedCategoricalGroup.group.map(_.acornTypeId)

  def gender: Set[String] = generalizedCategoricalGroup.group.map(_.gender)

  def maritalStatusId: Set[Option[Int]] = generalizedCategoricalGroup.group.map(_.maritalStatusId)

  def businessKey: (String, String) = (businessName, businessTown)

  def occupationId: Set[Option[Int]] = generalizedCategoricalGroup.group.map(_.occupationId)
}



case class CustomerToBusinessRating(maskedCustomerId: Long,
                                    businessKey: (String, String),
                                    visits: Int,
                                    amount: Double,
                                    merchantCategoryCode: String,
                                    numRatersPerBusiness: Int,
                                    totalAmountPerBusiness: Double,
                                    totalVisitsPerBusiness: Int,
                                    totalAmountPerCustomer: Double,
                                    totalVisitsOfCustomer: Int,
                                    totalBusinessesOfCustomer: Int
                                     )


case object CustomerToBusinessRating {
  def toSv(sep: String = "\t", listSep: String = ",")(rating: CustomerToBusinessRating): String = {

    import rating._
    List(maskedCustomerId,
      businessKey.productIterator.toList.mkString(listSep),
      visits,amount,merchantCategoryCode,
      numRatersPerBusiness,totalAmountPerBusiness,
      totalVisitsPerBusiness, totalAmountPerCustomer, totalVisitsOfCustomer, totalBusinessesOfCustomer).mkString(sep)
  }

  def fromSv(sep: String = "\t", listSep: String = ",")(line: String): CustomerToBusinessRating =
    line.split(sep, -1).toList match {
      case List(maskedCustomerId,
      businessKey,
      visits,
      amount,
      merchantCategoryCode,
      numRatersPerBusiness,
      totalAmountPerBusiness,
      totalVisitsPerBusiness,
      totalAmountPerCustomer,
      totalVisitsOfCustomer,
      totalBusinessesOfCustomer) =>

        CustomerToBusinessRating(
          maskedCustomerId.toLong,
          businessKey. split(listSep).toList  match {case List(a: String, b: String, _*) => (a, b)},
          visits.toInt,
          amount.toDouble,
          merchantCategoryCode,
          numRatersPerBusiness.toInt,
          totalAmountPerBusiness.toDouble,
          totalVisitsPerBusiness.toInt,
          totalAmountPerCustomer.toDouble,
          totalVisitsOfCustomer.toInt,
          totalBusinessesOfCustomer.toInt
        )
    }
}
