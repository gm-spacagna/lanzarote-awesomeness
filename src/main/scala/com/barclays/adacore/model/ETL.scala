package com.barclays.adacore.model

import com.barclays.adacore.AnonymizedRecord

case object ETL {
  def businessID(tx: AnonymizedRecord) = tx.businessName + "_" + tx.businessTown + "_" + tx.businessPostcode
}
