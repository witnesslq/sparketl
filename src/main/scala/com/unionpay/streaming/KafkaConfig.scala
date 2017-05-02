package com.unionpay.streaming

import com.unionpay.util.ConfigUtil
import net.ceedubs.ficus.readers.ArbitraryTypeReader._

/**
  * Created by ywp on 2016/7/25.
  */
object KafkaZKConfig {

  def getDefaultConf(configName: String = "kafkaZookeeper", rootNode: String = "config"): KafkaZKConfig = {
    import net.ceedubs.ficus.Ficus._
    val config = ConfigUtil.readClassPathConfig[ZkKafkaConf](configName, rootNode)
    config.buildConfig
  }
}


case class KafkaZKConfig(zkList: String, brokerList: String)

case class BasicConf(hosts: Seq[String], port: String)

case class ZkKafkaConf(zk: BasicConf, kafka: BasicConf) {

  def buildConfig: KafkaZKConfig = {
    val z = zk.hosts.map(z => s"$z:${zk.port}").mkString(",")
    val k = kafka.hosts.map(k => s"$k:${kafka.port}").mkString(",")
    KafkaZKConfig(z, k)
  }

}