package com.example

import java.util.Properties

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

class FavoriteColorCountsSpec extends FreeSpec with MustMatchers with BeforeAndAfterAll {

  var localStateDir = "local_state_data"
  val brokers       = "localhost:9092"

  class RecordProcessor extends RecordProcessorTrait[String, Long] {
    override def processRecord(record: ConsumerRecord[String, Long]): Unit =
      println(s"Get Message $record")
  }
  //  var s: KafkaLocalServer = _
  //  override def afterAll(): Unit = s.stop()

  private val DEFAULT_ZK_SESSION_TIMEOUT_MS    = 10 * 1000
  private val DEFAULT_ZK_CONNECTION_TIMEOUT_MS = 8 * 1000

  val zkUtils: ZkUtils =
    ZkUtils.apply(s"localhost:2181",
                  DEFAULT_ZK_SESSION_TIMEOUT_MS,
                  DEFAULT_ZK_CONNECTION_TIMEOUT_MS,
                  false)

  def createTopic(topic: String, partitions: Int, replication: Int, topicConfig: Properties): Unit =
    AdminUtils.createTopic(zkUtils,
                           topic,
                           partitions,
                           replication,
                           topicConfig,
                           RackAwareMode.Enforced)

  val testData = ColorTestData("colorcount-in", "colorcount-out")

  override def beforeAll(): Unit = {
    createTopic(testData.inputTopic, 1, 1, new Properties)
    createTopic(testData.outputTopic, 1, 1, new Properties)
    val sender = MessageSender[String, String](brokers,
                                               classOf[StringSerializer].getName,
                                               classOf[StringSerializer].getName)
    testData.inputValues foreach {
      case (k, v) =>
        sender.writeKeyValue(testData.inputTopic, k, v)
    }
    //    s = KafkaLocalServer(true, Some(localStateDir))
    //    s.start()
  }

  val properties = Map(
    StreamsConfig.APPLICATION_ID_CONFIG            -> "color_count_spec",
    StreamsConfig.BOOTSTRAP_SERVERS_CONFIG         -> "localhost:9092",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG        -> "earliest",
    StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG   -> Serdes.String().getClass,
    StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG -> Serdes.String().getClass,
  )
  val conf: Properties = properties.toProps

  val builder = FavoriteColorCounts.createTopology(testData.inputTopic, testData.outputTopic)

  "test" in {
    val topology = builder.build()
    println("topology: ")
    println(topology.describe())
    val streams = new KafkaStreams(topology, conf)
    streams.start()

    val listener = MessageListener(brokers,
                                   testData.outputTopic,
                                   "colorcountGroup",
                                   classOf[StringDeserializer].getName,
                                   classOf[LongDeserializer].getName,
                                   new RecordProcessor)

    val l = listener.waitUntilMinKeyValueRecordsReceived(testData.expectedCounts.size, 10000)

    streams.close()
  }

}

case class ColorTestData(inTopic: String, outTopic: String) {
  val inputTopic  = s"$inTopic.${scala.util.Random.nextInt(100)}"
  val outputTopic = s"$outTopic.${scala.util.Random.nextInt(100)}"

  val inputValues = Map(
    "bob"   -> "red",
    "alice" -> "blue",
    "eva"   -> "red",
    "colin" -> "red"
  )

  val expectedCounts: List[KeyValue[String, Long]] = List(
    new KeyValue("red", 1L),
    new KeyValue("blue", 1L),
    new KeyValue("red", 2L),
    new KeyValue("red", 3L)
  )
}
