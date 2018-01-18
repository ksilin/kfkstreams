package com.example

import com.lightbend.kafka.scala.streams.{ KTableS, StreamsBuilderS }
import org.apache.kafka.common.serialization.{ Serde, Serdes }
import org.apache.kafka.streams.kstream.Produced

object FavoriteColorCounts {

  val longSerde: Serde[Long] = Serdes.Long().asInstanceOf[Serde[Long]]

  val legalColors = Set("red", "blue")

  // assuming key is username & value is color
  def createTopology(sourceTopic: String, targetTopic: String): StreamsBuilderS = {
    println(s"creating builder for $sourceTopic / $targetTopic")
    val builder: StreamsBuilderS = new StreamsBuilderS

    val produced: Produced[String, Long] =
      Produced.`with`(Serdes.String(), longSerde)

    val msgs = builder.stream[String, String](sourceTopic)
    msgs
      .filter((k, v) => legalColors.contains(v))
      .selectKey((_, v) => v)
      .peek((k, v) => println(s"processing $k:$v"))
      .groupByKey()
      .count("byColors")
      .toStream
      .peek((k, v) => s"writing out: $k:$v ")
      .to(targetTopic, produced)

    builder
  }
//  val streams = new KafkaStreams(builder.build(), conf)
//  streams.start()

}
