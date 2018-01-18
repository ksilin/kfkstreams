package com.example

import java.util.Properties
import java.util.regex.Pattern

import com.lightbend.kafka.scala.streams.{ KTableS, StreamsBuilderS }
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ Serde, Serdes }
import org.apache.kafka.streams.{ KafkaStreams, StreamsConfig }
import org.apache.kafka.streams.kstream.Produced

object WordCountScalaApp extends App {

  val inTopic  = "wordcount-in"
  val outTopic = "wordcount-out"

  val properties = Map(
    StreamsConfig.APPLICATION_ID_CONFIG            -> "word_count_scala",
    StreamsConfig.BOOTSTRAP_SERVERS_CONFIG         -> "localhost:9092",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG        -> "earliest",
    StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG   -> Serdes.String().getClass,
    StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG -> Serdes.String().getClass,
  )
  val conf: Properties = properties.toProps

  // need to cast:
  // [error]  found   : org.apache.kafka.common.serialization.Serde[java.lang.Long]
  // [error]  required: org.apache.kafka.common.serialization.Serde[scala.Long]
  val longSerde: Serde[Long] = Serdes.Long().asInstanceOf[Serde[Long]]

  val builder = new StreamsBuilderS

  val textLines = builder.stream[String, String](inTopic)

  val pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS)

  val wordCounts: KTableS[String, Long] =
    textLines
      .flatMapValues(v => pattern.split(v.toLowerCase))
      .peek((k, v) => println(s"$k: $v"))
      .groupBy((k, v) => v) // convert to KTable
      .count()

  val produced: Produced[String, Long] =
    Produced.`with`(Serdes.String(), longSerde)

  wordCounts.toStream.to(outTopic, produced)
  val streams = new KafkaStreams(builder.build(), conf)
  streams.start()

}
