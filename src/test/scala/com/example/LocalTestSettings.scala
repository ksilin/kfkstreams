package com.example

import de.exellio.kafkabase.test.RecordProcessor
import kafka.utils.ZkUtils
import org.apache.kafka.clients.consumer.ConsumerRecord

trait LocalTestSettings {

  var localStateDir = "local_state_data"
  val brokers       = "localhost:9092"
  val zkHost        = s"localhost:2181"

  private val DEFAULT_ZK_SESSION_TIMEOUT_MS    = 10 * 1000
  private val DEFAULT_ZK_CONNECTION_TIMEOUT_MS = 8 * 1000

  val zkUtils: ZkUtils =
    ZkUtils.apply(zkHost,
                  DEFAULT_ZK_SESSION_TIMEOUT_MS,
                  DEFAULT_ZK_CONNECTION_TIMEOUT_MS,
                  isZkSecurityEnabled = false)

  class PrintingRecordProcessor extends RecordProcessor[String, Long] {
    override def processRecord(record: ConsumerRecord[String, Long]): Unit =
      println(s"processing message $record")
  }

}
