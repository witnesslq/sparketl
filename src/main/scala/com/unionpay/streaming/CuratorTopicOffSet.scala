package com.unionpay.streaming

import java.util.Properties

import com.unionpay.util.JsonUtil
import kafka.client.ClientUtils
import kafka.cluster.Broker
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.producer.ProducerConfig
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryUntilElapsed
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.spark.streaming.kafka.OffsetRange

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
  * Created by ywp on 2016/9/11.
  */
case class CuratorTopicOffSet(kafkaSavePath: String, conf: Map[String, String]) extends Serializable {

  private val curatorFramework = CuratorSingleton.getFrameWork


  def saveOffset(offsetRanges: Array[OffsetRange]) = {
    offsetRanges.foreach(o => {
      val topicPath = kafkaSavePath + "/" + o.topic
      val partitionPath = topicPath + "/" + o.partition
      if (Option(curatorFramework.checkExists().forPath(topicPath)).isEmpty) {
        val dirs = partitionPath.split("/")
        var parent = "/" + dirs(1)
        dirs.slice(2, dirs.length).map { d =>
          curatorFramework.create().forPath(parent, d.getBytes)
          parent = parent + "/" + d
        }
      }
    })
  }

  def getOffset(topics: Set[String]): Map[TopicAndPartition, Long] = {
    getOffSetFromZK getOrElse getOffSetFromKafka(topics)
  }

  def getOffSetFromZK: Option[Map[TopicAndPartition, Long]] = {
    Try {
      curatorFramework.getChildren.forPath(kafkaSavePath).toSet[String]
        .flatMap(topic => {
          val topicPath = kafkaSavePath + "/" + topic
          curatorFramework.getChildren.forPath(topicPath).toSet[String]
            .map(partition => {
              val offsetArray = curatorFramework.getData.forPath(topicPath + "/" + partition)
              val offset = new String(offsetArray.array.takeWhile(_ != 0))
              (new TopicAndPartition(topic, partition.toInt), offset.toLong)
            })
        }).toMap
    }.toOption
  }

  def getOffSetFromKafka(topics: Set[String]): Map[TopicAndPartition, Long] = {
    val properties = new Properties()
    properties.put("metadata.broker.list", conf.get("metadata.broker.list").get)

    val tps = topicAndPartitions(topics)

    println("tps:" + JsonUtil.toJson4sPrettyString(tps))

    val consumers = properties
      .getProperty("metadata.broker.list")
      .split(",")
      .map(broker => {
        val Array(host, port) = broker.split(":")
        new SimpleConsumer(host, port.toInt, 3000, 1000, "")
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
        //        BrokerEndPoint(brokerIndex, host, port.toInt)
      })

    println("brokers:" + JsonUtil.toJacksonPrettyString(brokers))

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

object CuratorSingleton {

  private lazy val curatorFramework = CuratorFrameworkFactory.builder().connectString(KafkaZKConfig.getDefaultConf().zkList)
    .connectionTimeoutMs(30000).sessionTimeoutMs(30000).retryPolicy(new RetryUntilElapsed(6000, 6000)).build()
  curatorFramework.start()


  def getFrameWork = {
    curatorFramework
  }
}