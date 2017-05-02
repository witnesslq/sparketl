package com.unionpay.streaming

/**
  * Created by ywp on 2016/7/25.
  */

import java.util.Properties

import kafka.client.ClientUtils
import kafka.cluster.Broker
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.producer.ProducerConfig
import org.apache.hadoop.yarn.lib.ZKClient
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.zookeeper._

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}


class ZKWatcher extends Watcher {
  override def process(event: WatchedEvent): Unit = {}
}

case class TopicOffset(kafkaSavePath: String, conf: Map[String, String]) extends Serializable {


  def saveOffset(offsetRanges: Array[OffsetRange]) = {
    val zkClient = new ZooKeeper(conf.get("zookeeper.connect").get, 30000, new ZKWatcher())
    offsetRanges.foreach(o => {
      val topicPath = kafkaSavePath + "/" + o.topic
      val partitionPath = topicPath + "/" + o.partition

      if (zkClient.exists(topicPath, false) == null) {
        Try {
          val dirs = partitionPath.split("/")
          var parent = "/" + dirs(1)
          dirs.slice(2, dirs.length).map { d =>
            zkClient.create(parent, d.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
              CreateMode.PERSISTENT);
            parent = parent + "/" + d
          }
        }.getOrElse("")
      }

      if (zkClient.exists(partitionPath, false) == null) {
        zkClient.create(partitionPath, o.fromOffset.toString.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
      }

      zkClient.setData(partitionPath, o.fromOffset.toString.getBytes(), -1)
    })
    zkClient.close()
  }

  /**
    * get offset from zk or kafka
    *
    * @param topics
    * @return
    */
  def getOffset(topics: Set[String]): Map[TopicAndPartition, Long] = {
    getOffSetFromZK().getOrElse(getOffSetFromKafka(topics))
  }

  // TODO: FIX: kafkasavepath exists and topic not exist will cause not found topic exception
  def getOffSetFromZK(): Option[Map[TopicAndPartition, Long]] = {
    Try {
      val zk = new ZKClient(conf.get("zookeeper.connect").get)
      zk.listServices(kafkaSavePath).toSet[String]
        .flatMap(topic => {
          val topicPath = kafkaSavePath + "/" + topic
          zk.listServices(topicPath).toSet[String]
            .map(partition => {
              val offset = zk.getServiceData(topicPath + "/" + partition)
              (new TopicAndPartition(topic, partition.toInt), offset.toLong)
            })
        })
        .toMap
    }.toOption
  }

  def getOffSetFromKafka(topics: Set[String]): Map[TopicAndPartition, Long] = {
    val properties = new Properties()
    properties.put("metadata.broker.list", conf.get("metadata.broker.list").get)
    val tps = topicAndPartitions(topics)

    val consumers = properties
      .getProperty("metadata.broker.list")
      .split(",")
      .map(broker => {
        val Array(host, port) = broker.split(":")
        new SimpleConsumer(host, port.toInt, 3000, 1000, "1")
      })

    tps.map(tp => {
      val offset = consumers.map { consumer =>
        Try {
          consumer.earliestOrLatestOffset(tp, -1l, 0)
        } match {
          case Success(x) => x
          case Failure(e) => println(s"kafka error ${e.getMessage}"); -1l
        }

      }.max
      (tp, offset)
    })
      .toMap
  }

  def topicAndPartitions(topics: Set[String]): Seq[TopicAndPartition] = {
    val properties = new Properties()
    properties.put("metadata.broker.list", conf.get("metadata.broker.list").get)

    var brokerIndex = -1
    val brokers = properties
      .getProperty("metadata.broker.list")
      .split(",")
      .toSeq
      .map(broker => {
        val Array(host, port) = broker.split(":")
        brokerIndex += 1
        Broker.createBroker(brokerIndex, s"""{"version":1,"host":"${host}","port":${port}}""")
      })

    val producer = new ProducerConfig(properties)

    ClientUtils.fetchTopicMetadata(topics, brokers.map(_.getBrokerEndPoint(SecurityProtocol.PLAINTEXT)), producer, 1)
      //    ClientUtils.fetchTopicMetadata(topics, brokers, producer, 1)
      .topicsMetadata
      .flatMap(tm => {
        tm.partitionsMetadata.map(pm => {
          TopicAndPartition(tm.topic, pm.partitionId)
        })
      })
      .seq
  }
}
