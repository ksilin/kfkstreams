package com.example

import java.util.Properties

import com.fasterxml.jackson.databind.JsonNode
import de.exellio.kafkabase.KafkaSettings
import de.exellio.kafkabase.test.{ KafkaTestHelper, MessageListener }
import org.apache.kafka.clients.admin.{ AdminClient, AdminClientConfig }
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.{ Serde, Serdes, StringDeserializer }
import org.apache.kafka.connect.json.{ JsonDeserializer, JsonSerializer }
import org.apache.kafka.streams.{ KafkaStreams, StreamsConfig }
import org.scalatest.{ BeforeAndAfterAll, FreeSpec, MustMatchers }

class BankBalanceOvoSpec
    extends FreeSpec
    with MustMatchers
    with BeforeAndAfterAll
    with LocalTestSettings
    with KafkaTestHelper {

  val names =
    List("Pedro", "Stanton", "Harold", "Ara", "Eleonore", "Hermelinda", "Devon", "Bethany")

  val inTopic           = "bank-balance-primer-in"
  val outTopic          = "bank-balance-primer-out"
  val intermediateTopic = s"$inTopic.-.$outTopic"

  val balance = new BankBalanceOvotech(brokers, names)

  val msgCount = 100

  val topoPropMap = Map(
    StreamsConfig.APPLICATION_ID_CONFIG       -> "bank_balance_ovo",
    StreamsConfig.BOOTSTRAP_SERVERS_CONFIG    -> brokers,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG   -> "earliest",
    StreamsConfig.PROCESSING_GUARANTEE_CONFIG -> StreamsConfig.EXACTLY_ONCE, // TODO try with and without
    // ESSENTIAL TO SEE PROCESSING FROM INTERMEDIATE TOPIC if MSG count is low
    // if left on (by default), much fewer msgs will reach intermediate topic
    //    StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG -> "0",
    StreamsConfig.COMMIT_INTERVAL_MS_CONFIG -> "100" // unfortunately, in combination with caching, this is not sufficient to push data downstream
  )
  val topoProps: Properties = topoPropMap.toProps

  val props = new Properties()
  props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  val admin: AdminClient = AdminClient.create(props)

  override def beforeAll(): Unit = {
    recreateTopic(zkHost, inTopic)
    recreateTopic(zkHost, outTopic)
    recreateTopic(zkHost, intermediateTopic)
  }

  "must create records" in {

    val listener = MessageListener(
      brokers,
      inTopic,
      "bankBalanceSpecGroup",
      classOf[StringDeserializer].getName,
      balance.jsonSerde.deserializer().getClass.getName,
      new PrintingRecordProcessor,
      "earliest"
    )

    val created: Seq[RecordMetadata] = balance.createRecords(inTopic, msgCount)
    created foreach { meta =>
      println(s"${meta.partition()}|${meta.offset()}|${meta.timestamp()}")
    }

    Thread.sleep(1000)
    val l = listener.waitUntilMinKeyValueRecordsReceived(msgCount, 10000)
  }

  "must create and run topology" in {

    val created: Seq[RecordMetadata] = balance.createRecords(inTopic, msgCount)
//    created foreach { meta =>
//      println(s"${meta.partition()}|${meta.offset()}|${meta.timestamp()}")
//    }

    Thread.sleep(1000)

    val topo = balance.createTopology(inTopic, outTopic).build()
    println("topo: ")
    println(topo.describe())
    val streams = new KafkaStreams(topo, topoProps)
    streams.cleanUp()
    streams.start()

    Thread.sleep(1000)
  }

  "must delete unused topics" in {
    val deleted: Unit = deleteTopicByPrefix(admin, "favourite")
  }

}
