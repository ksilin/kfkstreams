package com.example

import com.lightbend.kafka.scala.streams.{ KGroupedTableS, KStreamS, KTableS, StreamsBuilderS }
import org.apache.kafka.common.serialization.{ Serde, Serdes }
import org.apache.kafka.streams.kstream.Produced

object FavoriteColorCounts {

  val longSerde: Serde[Long] = Serdes.Long().asInstanceOf[Serde[Long]]

  val legalColors = Set("red", "blue")

  val produced: Produced[String, Long] =
    Produced.`with`(Serdes.String(), longSerde)

  // TODO - this topology correctly increases the values but does not decrease them as required, e.g.
  // x:red, y:blue => red:1 | blue:1
  // y: red => red:2 | but no blue:0 as would be correct
  // topology2 works correctly

  def createTopology(sourceTopic: String, targetTopic: String): StreamsBuilderS = {
    println(s"creating builder for $sourceTopic / $targetTopic")
    val builder: StreamsBuilderS = new StreamsBuilderS

    val msgs = builder.stream[String, String](sourceTopic)
    msgs
      .filter((_, v) => v.contains(","))
      .selectKey[String]((_, v) => v.split(",")(0).toLowerCase)
      .mapValues[String]((value: String) => value.split(",")(1).toLowerCase)
      .filter((_, v) => legalColors.contains(v))
      .selectKey((_, v) => v)
      .groupByKey()
      .count("byColors")
      .toStream
      .to(targetTopic, produced)

    builder
  }

  def createTopologyPeek(sourceTopic: String, targetTopic: String): StreamsBuilderS = {
    println(s"creating builder for $sourceTopic / $targetTopic")
    val builder: StreamsBuilderS = new StreamsBuilderS

    val msgs = builder.stream[String, String](sourceTopic)
    msgs
      .peek((k, v) => println(s"received $k:$v"))
      .filter((_, v) => v.contains(","))
      .selectKey[String]((_, v) => v.split(",")(0).toLowerCase)
      .mapValues[String](
        (value: String) => value.split(",").tail.headOption.map(_.toLowerCase).orNull
      )
      .filter((_, v) => legalColors.contains(v) || v == null)
      .peek((k, v) => println(s"filtered $k:$v"))
      .selectKey((_, v) => v)
      .peek((k, v) => println(s"selected $k:$v"))
      .groupByKey()
      .count("byColors")
      .toStream
      .peek((k, v) => s"writing out: $k:$v ") // the data gets written, but 'peek' does not print anything
      .to(targetTopic, produced)

    builder
  }

  def createTopology2(sourceTopic: String,
                      targetTopic: String,
                      intermediaryTopic: String): StreamsBuilderS = {
    println(s"creating builder for $sourceTopic / $targetTopic")
    val builder: StreamsBuilderS = new StreamsBuilderS

    val msgs: KStreamS[String, String] = builder.stream[String, String](sourceTopic)

    val usersAndColours: KStreamS[String, String] = msgs
    // 1 - we ensure that a comma is here as we will split on it
      .filter((key: String, value: String) => value.contains(","))
      // 2 - we select a key that will be the user id (lowercase for safety)
      .selectKey[String]((key: String, value: String) => value.split(",")(0).toLowerCase)
      // 3 - we get the colour from the value (lowercase for safety)
      .mapValues[String]((value: String) => value.split(",")(1).toLowerCase)
      // 4 - we filter undesired colours (could be a data sanitization step)
      .filter((user: String, colour: String) => List("green", "blue", "red").contains(colour))

    usersAndColours.to(intermediaryTopic)

    val usersAndColoursTable: KTableS[String, String] = builder.table(intermediaryTopic)

    // step 3 - we count the occurences of colours
    val favouriteColours1: KGroupedTableS[String, String] =
      usersAndColoursTable.groupBy((user: String, colour: String) => (colour, colour))
    val favouriteColours = favouriteColours1.count()

    // 6 - we output the results to a Kafka Topic - don't forget the serializers
    favouriteColours.toStream
      .peek((k, v) => println(s"writing out: $k:$v "))
      .to(targetTopic, produced)

    builder
  }

  def createTopology3(sourceTopic: String,
                      targetTopic: String,
                      intermediaryTopic: String): StreamsBuilderS = {
    println(s"creating builder for $sourceTopic / $targetTopic")
    val builder: StreamsBuilderS = new StreamsBuilderS

    val msgs: KStreamS[String, String] = builder.stream[String, String](sourceTopic)

    val usersAndColours: KStreamS[String, String] = msgs
    // 1 - we ensure that a comma is here as we will split on it
      .filter((key: String, value: String) => value.contains(","))
      // 2 - we select a key that will be the user id (lowercase for safety)
      .selectKey[String]((key: String, value: String) => value.split(",")(0).toLowerCase)
      // 3 - we get the colour from the value (lowercase for safety)
      .mapValues[String]((value: String) => value.split(",")(1).toLowerCase)
      // 4 - we filter undesired colours (could be a data sanitization step)
      .filter((user: String, colour: String) => List("green", "blue", "red").contains(colour))

    usersAndColours.to(intermediaryTopic)

    val usersAndColoursTable: KTableS[String, String] = builder.table(intermediaryTopic)

    // step 3 - we count the occurences of colours
//    val favouriteColours1: KGroupedTableS[String, String] =
//      usersAndColoursTable.groupBy((user: String, colour: String) => (colour, colour))
//    val favouriteColours = favouriteColours1.count()

    // 6 - we output the results to a Kafka Topic - don't forget the serializers
    val produced2: Produced[String, String] =
      Produced.`with`(Serdes.String(), Serdes.String())

    usersAndColoursTable
      .mapValues(identity)
      .toStream
      .peek((k, v) => println(s"writing out: $k:$v "))
      .to(targetTopic, produced2)

    builder
  }

}
