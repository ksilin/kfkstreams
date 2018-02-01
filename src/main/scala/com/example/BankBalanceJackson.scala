package com.example

import java.time.OffsetDateTime

import com.example.BankBalanceData._
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.{ JsonNode, ObjectMapper }
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.lightbend.kafka.scala.streams.StreamsBuilderS
import de.exellio.kafkabase.test.MessageSender
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.{ Serde, Serdes }
import org.apache.kafka.connect.json.{ JsonDeserializer, JsonSerializer }
import org.apache.kafka.streams.Consumed
import org.apache.kafka.streams.kstream.{ Produced, Serialized }

import scala.collection.immutable
import scala.util.Random

class BankBalanceJackson(hosts: String, names: Seq[String]) {

  val jsonSerde: Serde[JsonNode] = Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer())

  val invalidTransfer = BankTransfer("INVALID", 0L, OffsetDateTime.now())

  val produced: Produced[String, JsonNode]     = Produced.`with`(Serdes.String(), jsonSerde)
  val consumed: Consumed[String, JsonNode]     = Consumed.`with`(Serdes.String(), jsonSerde)
  val serialized: Serialized[String, JsonNode] = Serialized.`with`(Serdes.String(), jsonSerde)
  val random                                   = new Random

  // TODO - rate limit - use akka streams?
  def createRecords(topic: String, count: Int): Seq[RecordMetadata] = {

    val keySerializer = Serdes.String.serializer()

    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper
      .registerModule(DefaultScalaModule)
      .registerModule(new JavaTimeModule())

    val sender = MessageSender[String, JsonNode](hosts, keySerializer, jsonSerde.serializer())
    val strMsgs: immutable.Seq[(String, JsonNode)] = (1 to count) map { _ =>
      val msg =
        BankTransfer(names(random.nextInt(names.length)), random.nextInt(100), OffsetDateTime.now())
      (msg.name, mapper.valueToTree(msg))
    }
    sender.batchWriteKeyValue(topic, strMsgs)
  }

  def createTopology(inTopic: String, outTopic: String): StreamsBuilderS = {

    val intermediaryTopic = s"$inTopic.-.$outTopic"

    val builder: StreamsBuilderS = new StreamsBuilderS
    val read                     = builder.stream[String, JsonNode](inTopic, consumed)

    read
      .groupByKey(serialized)
      .aggregate[JsonNode](() => JsonNodeFactory.instance.objectNode(),
                           (name: String, transferJson: JsonNode, balanceJson: JsonNode) =>
                             transferJson)
      .toStream
      .peek((k, v) => println(s"storing in intermediate: $k:$v"))
      .to(intermediaryTopic, produced)
    builder
  }

}
