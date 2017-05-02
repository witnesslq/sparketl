package com.unionpay.streaming

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient

/**
  * Created by ywp on 2016/7/25.
  */
object KafkaTopicAutoCreator {

  //    private lazy val zkClient = new ZkClient(KafkaZKConfig.getDefaultConf().zkList, 10000, 10000)
  private lazy val zkUtils = ZkUtils(KafkaZKConfig.getDefaultConf().zkList, 10000, 10000, false)
  private lazy val partitioner = 5
  private lazy val replicationFactor = 1

  def createTopics(topics: Set[String]) = {

    topics.foreach(topic => {
      /* if (!AdminUtils.topicExists(zkClient, topic)) {
         AdminUtils.createTopic(zkClient, topic, partitioner, replicationFactor)
       }*/

      if (!AdminUtils.topicExists(zkUtils, topic)) {
        AdminUtils.createTopic(zkUtils, topic, partitioner, replicationFactor)
      }
    })
  }

}
