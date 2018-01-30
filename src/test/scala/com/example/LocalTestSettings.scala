package com.example

import kafka.utils.ZkUtils

trait LocalTestSettings {

  var localStateDir  = "local_state_data"
  val brokers        = "localhost:9092"
  private val zkHost = s"localhost:2181"

  private val DEFAULT_ZK_SESSION_TIMEOUT_MS    = 10 * 1000
  private val DEFAULT_ZK_CONNECTION_TIMEOUT_MS = 8 * 1000

  val zkUtils: ZkUtils =
    ZkUtils.apply(zkHost,
                  DEFAULT_ZK_SESSION_TIMEOUT_MS,
                  DEFAULT_ZK_CONNECTION_TIMEOUT_MS,
                  isZkSecurityEnabled = false)

}
