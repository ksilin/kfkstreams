package com.example

import java.time.OffsetDateTime

import com.fasterxml.jackson.databind.{ JsonNode, ObjectMapper }
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.lightbend.kafka.scala.streams.{ KTableS, StreamsBuilderS }
import de.exellio.kafkabase.test.MessageSender
import io.circe.syntax._
import io.circe.generic.auto._
import io.circe.java8.time._
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.{ Serde, Serdes }
import org.apache.kafka.connect.json.{ JsonDeserializer, JsonSerializer }
import org.apache.kafka.streams.kstream.{ Produced, Serialized }

import scala.collection.immutable
import scala.util.Random

case class BankTransfer(name: String, amount: Long, time: OffsetDateTime)
case class BankBalance(name: String,
                       txCount: Int = 0,
                       balance: Long = 0,
                       latestTransaction: OffsetDateTime = OffsetDateTime.MIN)

class BankBalanceA(hosts: String, names: Seq[String]) {

  val jsonSerde: Serde[JsonNode] = Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer())

  val longSerde: Serde[Long] = Serdes.Long().asInstanceOf[Serde[Long]]

  val invalidTransfer = BankTransfer("INVALID", 0L, OffsetDateTime.now())

  val produced: Produced[String, Long] =
    Produced.`with`(Serdes.String(), longSerde)
  val random = new Random

  // TODO - rate limit - use akka streams?
  def createRecords(topic: String, count: Int): Seq[RecordMetadata] = {

    val keySerializer   = Serdes.String.serializer().getClass.getName
    val valueSerializer = jsonSerde.serializer().getClass.getName

    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

    val sender = new MessageSender[String, JsonNode](hosts, keySerializer, valueSerializer)
    val strMsgs: immutable.Seq[(String, JsonNode)] = (1 to count) map { i =>
      val msg =
        BankTransfer(names(random.nextInt(names.length)), random.nextInt(100), OffsetDateTime.now())
      (msg.name, mapper.valueToTree(msg))
    }
    sender.batchWriteKeyValue(topic, strMsgs)
  }

  def createTopology(inTopic: String, outTopic: String): StreamsBuilderS = {

    val intermediaryTopic = s"$inTopic.-.$outTopic"

    // passing the Serialized is optional but recommended
    val groupBySerialized = Serialized.`with`[String, JsonNode](Serdes.String(), jsonSerde)

    val builder: StreamsBuilderS = new StreamsBuilderS
    builder
      .stream[String, JsonNode](inTopic)
      .peek((k, v) => println(s"starting: $k:$v"))
      .groupByKey(groupBySerialized) // no more peeking
//      .aggregate[String](
//        () => BankBalance("someName").asJson.noSpaces,
//        (name: String, transferJson: String, balanceJson: String) => {
//          val transfer =
//            io.circe.parser.decode[BankTransfer](transferJson).getOrElse(invalidTransfer)
//          val aggBalance = io.circe.parser.decode[BankBalance](balanceJson).right.get // balance must be decodable
//          BankBalance(name, aggBalance.balance + transfer.amount, transfer.time).asJson.noSpaces
//        }
//      )
      .count()
      .toStream
      .peek((k, v) => println(s"storing in intermediate: $k:$v"))
      .to(intermediaryTopic)
//    val tbl: KTableS[String, String] = builder.table(intermediaryTopic)
//    tbl.builder
    builder
  }

}
