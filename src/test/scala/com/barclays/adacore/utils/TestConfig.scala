package com.barclays.adacore.utils

import org.specs2.matcher.Parameters

// TODO: Read those values from config file.
trait TestConfig {
  val minTestsOk = 10
  val params = Parameters(minTestsOk = minTestsOk)
}

