package com.example

import java.time.OffsetDateTime

object BankBalanceData {

  case class BankTransfer(name: String, amount: Long, time: OffsetDateTime)
  case class BankBalance(name: String,
                         amount: Long = 0,
                         latestTransaction: OffsetDateTime = OffsetDateTime.MIN)

}
