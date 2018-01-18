package com.example

import java.{ lang, util }
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.{ Metric, MetricName }
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{ KafkaStreams, StreamsBuilder, StreamsConfig, Topology }
import org.apache.kafka.streams.kstream.{ KStream, KTable, Produced }

import collection.JavaConverters._

object WordCountApp extends App {

  // https://docs.confluent.io/current/streams/developer-guide/write-streams.html

  println("starting")

  // also see https://github.com/Exellio/prozess/blob/master/src/test/scala/de/exellio/prozess/KafkaStreamsTestHelper.scala
  val properties = Map(
    StreamsConfig.APPLICATION_ID_CONFIG            -> "word_count_2",
    StreamsConfig.BOOTSTRAP_SERVERS_CONFIG         -> "localhost:9092",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG        -> "earliest",
    StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG   -> Serdes.String().getClass,
    StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG -> Serdes.String().getClass,
  )
  val conf: Properties = properties.toProps

  // xValues methods accept a  ValueMapper<V, VR> {VR apply(final V value);}
  // TODO - create topics before running the streams app
  // bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic word-count-input
  private val sourceTopic = "word-count-input"
  private val targetTopic = "word-count-output"

  val builder                            = new StreamsBuilder()
  val wcInput: KStream[String, String]   = builder.stream(sourceTopic)
  val lowercase: KStream[String, String] = wcInput.mapValues(_.toLowerCase)
  val split: KStream[String, String] =
    lowercase.flatMapValues[String](_.split("\\s+").toIterable.asJava)
  val valueAsKey: KStream[String, String] = split.selectKey((_, v) => v)
  val counted: KTable[String, lang.Long]  = valueAsKey.groupByKey().count() // "wordcounts") - explicitly defining a store is deprecated

  val produced: Produced[String, lang.Long] =
    Produced.`with`(Serdes.String(), Serdes.Long(), null)
  // course uses the deprecated variant counted.to("store")
  val writeBack: Unit = counted
    .toStream()
    .to(targetTopic, Produced.`with`(Serdes.String(), Serdes.Long(), null))

  val topology: Topology = builder.build()
  // course advises for teh deprecated variant new KafkaStreams(builder.build(), conf)
  val streams: KafkaStreams = new KafkaStreams(topology, conf)
  streams.start()

  // get metrics with timeout, somehow like this
  // TODO - investigate: https://docs.confluent.io/current/streams/monitoring.html
  val metrics: util.Map[MetricName, _ <: Metric] = streams.metrics()
  println("metrics: ")
  metrics.asScala foreach {
    case (name, metric) =>
      println(s"$name, ${metric.metricValue()}")
  }

  // should pring the topology, but only prints the current threads & tasks
  println("streams")
  println(streams)
  // does this also print the topology?
  println("topology")
  println(topology.describe())

  println("stopping")

//  def toProps(config: Map[String, AnyRef]): Properties = config.foldLeft(new Properties) {
//    case (props, (k, v)) => props.put(k, v); props
//  }

  // required to prevent errors and inconsistent state on exit
  sys.addShutdownHook(streams.close())

  // expected log out: StreamsConfigValues
  // Creating consumer client - group.id = word_count_1
  // Creating restore consumer client - will try to restore it's state after restart
  // Creating shared producer client

  // the latency from input to output seesm rather high - feels like about 10sec. Is this normal? Why is it so high? Can I reduce it?

  // kafka streams code can be debugged directly from IJ - great

  // internal topics
  // * partitioning topics - if you are transforming the key
  // * changelog topics - aggregations - stores compacted data
  // prefixed by application.id
  // do not touch them
}
