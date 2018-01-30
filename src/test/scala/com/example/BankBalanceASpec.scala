package com.example

import de.exellio.kafkabase.test.{ KafkaTestHelper, MessageListener }
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.{ LongDeserializer, Serdes, StringDeserializer }
import org.scalatest.{ BeforeAndAfterAll, FreeSpec, MustMatchers }

class BankBalanceASpec
    extends FreeSpec
    with MustMatchers
    with BeforeAndAfterAll
    with LocalTestSettings
    with KafkaTestHelper {

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

  val kDeser = Serdes.String.deserializer()

  val msgCount = 100

  override def beforeAll(): Unit = recreateTopic(zkHost, topic)

  "must create records" in {

    val listener = MessageListener(brokers,
      topic,
      "bankBalanceSpecGroup",
      classOf[StringDeserializer].getName,
      classOf[StringDeserializer].getName,
      new PrintingRecordProcessor, "earliest")

    val created: Seq[RecordMetadata] = balance.createRecords(topic, msgCount)
    created foreach { meta =>
      println(s"${meta.partition()}|${meta.offset()}|${meta.timestamp()}")
    }

    Thread.sleep(1000)
    val l = listener.waitUntilMinKeyValueRecordsReceived(msgCount, 10000)
  }

}
