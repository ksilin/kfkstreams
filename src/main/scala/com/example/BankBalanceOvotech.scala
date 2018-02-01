package com.example

import java.time.OffsetDateTime

import com.ovoenergy.kafka.serialization.core._
import com.ovoenergy.kafka.serialization.circe._
import org.apache.kafka.common.serialization.{ Deserializer, Serializer }

// Import the Circe generic support
import io.circe.generic.auto._
import io.circe.java8.time._
import io.circe.syntax._

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.CommonClientConfigs._
import com.example.BankBalanceData._
import com.lightbend.kafka.scala.streams.StreamsBuilderS
import de.exellio.kafkabase.test.MessageSender
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.{ Serde, Serdes }
import org.apache.kafka.connect.json.{ JsonDeserializer, JsonSerializer }
import org.apache.kafka.streams.Consumed
import org.apache.kafka.streams.kstream.{ Produced, Serialized }

import scala.collection.immutable
import scala.util.Random

class BankBalanceOvotech(hosts: String, names: Seq[String]) {

  private val ser: Serializer[BankTransfer]     = circeJsonSerializer[BankTransfer]
  private val deser: Deserializer[BankTransfer] = circeJsonDeserializer[BankTransfer]
  val jsonSerde                                 = Serdes.serdeFrom(ser, deser)

  val invalidTransfer = BankTransfer("INVALID", 0L, OffsetDateTime.now())

  val random = new Random

  def createRecords(topic: String, count: Int): Seq[RecordMetadata] = {
    val sender = MessageSender[String, BankTransfer](hosts, Serdes.String.serializer(), ser)
    val strMsgs = (1 to count) map { i =>
      val msg =
        BankTransfer(names(random.nextInt(names.length)), random.nextInt(100), OffsetDateTime.now())
      (msg.name, msg)
    }
    sender.batchWriteKeyValue(topic, strMsgs)
  }
}
