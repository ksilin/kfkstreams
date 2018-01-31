package com.example

import java.time.OffsetDateTime

import com.fasterxml.jackson.databind.JsonNode
import com.lightbend.kafka.scala.streams.{KTableS, StreamsBuilderS}
import de.exellio.kafkabase.test.MessageSender
import io.circe.syntax._
import io.circe.generic.auto._
import io.circe.java8.time._
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.connect.json.{JsonDeserializer, JsonSerializer}
import org.apache.kafka.streams.kstream.Produced

import scala.util.Random

case class BankTransfer(name: String, amount: Long, time: OffsetDateTime)
case class BankBalance(name: String,
                       amount: Long = 0,
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
    val serializer = Serdes.String.serializer().getClass.getName
    val sender     = new MessageSender[String, String](hosts, serializer, serializer)
    val strMsgs = (1 to count) map { i =>
      val msg =
        BankTransfer(names(random.nextInt(names.length)), random.nextInt(100), OffsetDateTime.now())
      (msg.name, msg.asJson.noSpaces)
    }
    sender.batchWriteKeyValue(topic, strMsgs)
  }

  def createTopology(inTopic: String, outTopic: String): StreamsBuilderS = {

    val intermediaryTopic = s"$inTopic.-.$outTopic"

    val builder: StreamsBuilderS = new StreamsBuilderS
    builder
      .stream[String, String](inTopic)
      .peek((k, v) => println(s"starting: $k:$v"))
      .groupByKey() // no more peeking
      .aggregate[String](
        () => BankBalance("someName").asJson.noSpaces,
        (name: String, transferJson: String, balanceJson: String) => {
          val transfer =
            io.circe.parser.decode[BankTransfer](transferJson).getOrElse(invalidTransfer)
          val aggBalance = io.circe.parser.decode[BankBalance](balanceJson).right.get // balance must be decodable
          BankBalance(name, aggBalance.amount + transfer.amount, transfer.time).asJson.noSpaces
        }
      )
      .toStream
      .peek((k, v) => println(s"storing in intermediate: $k:$v"))
      .to(intermediaryTopic)
//    val tbl: KTableS[String, String] = builder.table(intermediaryTopic)
//    tbl.builder
    builder
  }

}
