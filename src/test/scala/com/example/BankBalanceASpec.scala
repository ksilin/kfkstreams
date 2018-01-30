package com.example

import org.apache.kafka.clients.producer.RecordMetadata
import org.scalatest.{ BeforeAndAfterAll, FreeSpec, MustMatchers }

class BankBalanceASpec
    extends FreeSpec
    with MustMatchers
    with BeforeAndAfterAll
    with LocalTestSettings {

  val names = List("Pedro",
                   "Stanton",
                   "Harold",
                   "Quyen",
                   "Ara",
                   "Eleonore",
                   "Jolyn",
                   "Hermelinda",
                   "Devon",
                   "Bethany")

  val topic = "bank-balance-primer"

  val balance = new BankBalanceA(brokers, names)

  "must create records" in {

    val created: Seq[RecordMetadata] = balance.createRecords(topic, 100)
    created foreach { meta =>
      println(s"${meta.partition()}|${meta.offset()}|${meta.timestamp()}")
    }
  }

}
