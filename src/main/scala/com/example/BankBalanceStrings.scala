package com.example

import java.time.OffsetDateTime

import com.example.BankBalanceData._
import com.lightbend.kafka.scala.streams.{ KTableS, StreamsBuilderS }
import de.exellio.kafkabase.test.MessageSender
import io.circe.syntax._
import io.circe.generic.auto._
import io.circe.java8.time._
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.{ Serde, Serdes }
import org.apache.kafka.streams.kstream.Produced

import scala.util.Random

class BankBalanceStrings(hosts: String, names: Seq[String]) {

  val longSerde: Serde[Long] = Serdes.Long().asInstanceOf[Serde[Long]]

  val invalidTransfer = BankTransfer("INVALID", 0L, OffsetDateTime.now())

  val produced: Produced[String, Long] =
    Produced.`with`(Serdes.String(), longSerde)
  val random = new Random

  // TODO - rate limit - use akka streams?
  def createRecords(topic: String, count: Int): Seq[RecordMetadata] = {
    val serializer = Serdes.String.serializer().getClass.getName
    val sender     = MessageSender[String, String](hosts, serializer, serializer)
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
