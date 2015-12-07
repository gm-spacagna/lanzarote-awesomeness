package com.barclays.adacore

case class RawAccount(customerId: Long, balance: Option[Double], age: Option[Int], onlineActive: Boolean,
                      income: Option[Double], grossIncome: Double, postalSector: Option[String], acornTypeId: Int,
                      gender: String, maritalStatusId: Option[Int], occupationId: Option[Int])


