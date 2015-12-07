package com.barclays.adacore.utils

case object Logger {
  def getLogger(level: org.apache.log4j.Level): org.apache.log4j.Logger = {
    val logger = org.apache.log4j.Logger.getLogger("ADA")
    logger.setLevel(level)
    logger
  }

  def apply(): org.apache.log4j.Logger = infoLevel
  def infoLevel = getLogger(org.apache.log4j.Level.INFO)
  def debugLevel = getLogger(org.apache.log4j.Level.DEBUG)
}
