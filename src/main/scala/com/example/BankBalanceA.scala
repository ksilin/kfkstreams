package com.example

import java.time.OffsetDateTime

import de.exellio.kafkabase.KafkaSettings
import de.exellio.kafkabase.test.MessageSender
import io.circe.syntax._
import io.circe.generic.auto._
import io.circe.java8.time._
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.Serdes

import scala.util.Random

case class BankTransfer(name: String, amount: Long, time: OffsetDateTime)

class BankBalanceA(hosts: String, names: Seq[String]) {

  val random = new Random

  def createRecords(topic: String, count: Int): Seq[RecordMetadata] = {
    val serializer = Serdes.String.serializer().getClass.getName
    val sender     = new MessageSender[String, String](hosts, serializer, serializer)
    val strMsgs = (1 to count) map { i =>
      val msg =
        BankTransfer(names(random.nextInt(names.length)), random.nextInt(100), OffsetDateTime.now())
      msg.asJson.noSpaces
    }
    sender.batchWriteValue(topic, strMsgs)
  }

}
