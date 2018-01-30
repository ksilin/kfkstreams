package com.example

import java.util.Properties

import de.exellio.kafkabase.test.{ MessageListener, MessageSender, RecordProcessor }
import kafka.admin.{ AdminUtils, RackAwareMode }
import kafka.utils.ZkUtils
import org.apache.kafka.clients.consumer.{ ConsumerConfig, ConsumerRecord }
import org.apache.kafka.common.serialization.{
  LongDeserializer,
  Serdes,
  StringDeserializer,
  StringSerializer
}
import org.apache.kafka.streams.{ KafkaStreams, KeyValue, StreamsConfig }
import org.scalatest.{ BeforeAndAfterAll, FreeSpec, MustMatchers }

import scala.util.Try

class FavoriteColorCountsSpec
    extends FreeSpec
    with MustMatchers
    with BeforeAndAfterAll
    with LocalTestSettings {

  class PrintingRecordProcessor extends RecordProcessor[String, Long] {
    override def processRecord(record: ConsumerRecord[String, Long]): Unit =
      println(s"Get Message $record")
  }

  val inputTopic        = "colorcount-in"
  val outputTopic       = "colorcount-out"
  val intermediateTopic = "user-keys-and-colours-scala"
  //  var s: KafkaLocalServer = _
  override def afterAll(): Unit = {
//    deleteTopic(inputTopic)
//    deleteTopic(outputTopic)
//    deleteTopic(intermediateTopic)
  }

  def deleteTopic(topic: String): Unit = AdminUtils.deleteTopic(zkUtils, topic)

  def createTopic(topic: String, partitions: Int, replication: Int, topicConfig: Properties): Unit =
    AdminUtils.createTopic(zkUtils,
                           topic,
                           partitions,
                           replication,
                           topicConfig,
                           RackAwareMode.Enforced)

  override def beforeAll(): Unit = {
    Try { deleteTopic(inputTopic) }
    Try { deleteTopic(outputTopic) }
    Try { deleteTopic(intermediateTopic) }
    createTopic(inputTopic, 1, 1, new Properties)
    createTopic(outputTopic, 1, 1, new Properties)
    createTopic(intermediateTopic, 1, 1, new Properties)
//      case (k, v) =>
//        sender.writeKeyValue(testData.inputTopic, k, v)
//    }
    //    s = KafkaLocalServer(true, Some(localStateDir))
    //    s.start()
  }

  val properties = Map(
    StreamsConfig.APPLICATION_ID_CONFIG            -> "color_count_spec",
    StreamsConfig.BOOTSTRAP_SERVERS_CONFIG         -> brokers,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG        -> "earliest",
    StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG   -> Serdes.String().getClass,
    StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG -> Serdes.String().getClass,
    StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG -> "0", // THIS IS ESSENTIAL TO SEE ANY PROCESSING FROM INTERMEDIATE TOPIC
    StreamsConfig.COMMIT_INTERVAL_MS_CONFIG        -> "100" // unfortunately, in combination with caching, this is not sufficient to push data downstream
  )
  val conf: Properties = properties.toProps

  "test" in {
    val sender = MessageSender[String, String](brokers,
                                               classOf[StringSerializer].getName,
                                               classOf[StringSerializer].getName)
    ColorTestData.inputValues foreach { v =>
      sender.writeValue(inputTopic, v)
    }
    val builder  = FavoriteColorCounts.createTopology2(inputTopic, outputTopic, intermediateTopic)
    val topology = builder.build()
    println("topology: ")
    println(topology.describe())
    val streams = new KafkaStreams(topology, conf)

    streams.cleanUp() // dont do this in prod
    streams.start()

    val listener = MessageListener(brokers,
                                   outputTopic,
                                   "colorcountGroup",
                                   classOf[StringDeserializer].getName,
                                   classOf[LongDeserializer].getName,
                                   new PrintingRecordProcessor)

    val l = listener.waitUntilMinKeyValueRecordsReceived(ColorTestData.expectedCounts.size, 10000)

    streams.close()
    sys.addShutdownHook(streams.close())
  }

  "test2" in {
    val sender = MessageSender[String, String](brokers,
                                               classOf[StringSerializer].getName,
                                               classOf[StringSerializer].getName)
    ColorTestData.inputKeyValues foreach {
      case (k, v) =>
        sender.writeKeyValue(inputTopic, k, v)
    }
    val builder  = FavoriteColorCounts.createTopology(inputTopic, outputTopic)
    val topology = builder.build()
    println("topology: ")
    println(topology.describe())
    val streams = new KafkaStreams(topology, conf)
    streams.cleanUp()
    streams.start()

    val listener = MessageListener(brokers,
                                   outputTopic,
                                   "colorcountGroup",
                                   classOf[StringDeserializer].getName,
                                   classOf[LongDeserializer].getName,
                                   new PrintingRecordProcessor)

    val l = listener.waitUntilMinKeyValueRecordsReceived(ColorTestData.expectedCounts.size, 10000)
    l foreach println
    streams.close()
    sys.addShutdownHook(streams.close())
  }

  "just run - external data" in {
    val builder =
      FavoriteColorCounts.createTopologyPeek("favourite-colour-input", "favourite-colour-output") //,
//                                          "user-keys-and-colours")
    val topology = builder.build()
    println("topology: ")
    println(topology.describe())
    val streams = new KafkaStreams(topology, conf)
    streams.cleanUp()
    streams.start()

    Thread.sleep(600000)
    sys.addShutdownHook(streams.close())
  }
}

object ColorTestData {
  val inputKeyValues = Map(
    "bob"   -> "red",
    "alice" -> "blue",
    "eva"   -> "red",
    "colin" -> "red"
  )

  val inputValues = List(
    "bob,red",
    "alice,blue",
    "eva,red",
    "colin,red"
  )

  val expectedCounts: List[KeyValue[String, Long]] = List(
    new KeyValue("red", 1L),
    new KeyValue("blue", 1L),
    new KeyValue("red", 2L),
    new KeyValue("red", 3L)
  )
}
