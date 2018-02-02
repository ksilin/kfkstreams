package com.example

import java.time.OffsetDateTime

import com.ovoenergy.kafka.serialization.core._
import com.ovoenergy.kafka.serialization.circe._
import org.apache.kafka.common.serialization.{ Deserializer, Serializer }

// Import the Circe generic support
import io.circe.generic.auto._
import io.circe.java8.time._
import io.circe.syntax._

import org.apache.kafka.clients.CommonClientConfigs._
import com.example.BankBalanceData._
import com.lightbend.kafka.scala.streams.StreamsBuilderS
import de.exellio.kafkabase.test.MessageSender
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.{ Serde, Serdes }
import org.apache.kafka.connect.json.{ JsonDeserializer, JsonSerializer }
import org.apache.kafka.streams.Consumed
import org.apache.kafka.streams.kstream.{ Produced, Serialized }

import scala.util.Random

class BankBalanceOvotech(hosts: String, names: Seq[String]) {

  private val ser: Serializer[BankTransfer]     = circeJsonSerializer[BankTransfer]
  private val deser: Deserializer[BankTransfer] = circeJsonDeserializer[BankTransfer]
  private val jsonSerde                         = Serdes.serdeFrom(ser, deser)

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

  val consumed: Consumed[String, BankTransfer]     = Consumed.`with`(Serdes.String, jsonSerde)
  val longSerde: Serde[Long]                       = Serdes.Long().asInstanceOf[Serde[Long]]
  val produced: Produced[String, Long]             = Produced.`with`(Serdes.String, longSerde)
  val serialized: Serialized[String, BankTransfer] = Serialized.`with`(Serdes.String, jsonSerde)

  def createTopology(inTopic: String, outTopic: String): StreamsBuilderS = {

    val builder = new StreamsBuilderS
    builder
      .stream[String, BankTransfer](inTopic, consumed)
      .peek((k, v) => println(s"starting: $k:$v"))
      .groupByKey(serialized) // no more peeking
      .count("tempStore", Some(Serdes.String()))
      .toStream
      .peek((k, v) => println(s"writing: $k:$v"))
      .to(outTopic, produced)
    builder
  }
}
