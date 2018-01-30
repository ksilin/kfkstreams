package com.example

import java.time.OffsetDateTime

import com.lightbend.kafka.scala.streams.StreamsBuilderS
import de.exellio.kafkabase.test.MessageSender
import io.circe.syntax._
import io.circe.generic.auto._
import io.circe.java8.time._
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.{ Serde, Serdes }
import org.apache.kafka.streams.kstream.Produced

import scala.util.Random

case class BankTransfer(name: String, amount: Long, time: OffsetDateTime)

class BankBalanceA(hosts: String, names: Seq[String]) {

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

    val intermediaryTopic = s"$inTopic<->$outTopic"

    val builder: StreamsBuilderS = new StreamsBuilderS
    builder
      .stream[String, String](inTopic)
      .mapValues(v => io.circe.parser.decode[BankTransfer](v).getOrElse(invalidTransfer))
      .selectKey((_, v) => v.name)
      .to(intermediaryTopic)

    val tbl = builder.table(intermediaryTopic)

    builder
  }

}
