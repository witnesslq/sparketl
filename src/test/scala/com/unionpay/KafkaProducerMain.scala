package com.unionpay

import java.util.Properties

import com.unionpay.util.JsonUtil
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.collection.immutable
import scala.util.Random

/**
  * Created by ywp on 2016/7/25.
  */
object KafkaProducerMain {


  private lazy val types = Seq("01", "02", "03", "04", "05")
  private lazy val values = Seq("v1", "v2", "v3", "v4")
  private lazy val input = Seq("i1", "i2", "i3", "i4")

  def main(args: Array[String]) {

    val props = new Properties()
    props.put("zk.connect", "127.0.0.1:2181")
    props.setProperty("metadata.broker.list", "localhost:9092")
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)
    var count = 0
    while (true) {

      val ty = getType
      val map = Map(
        "userId" -> getUserId,
        "type" -> ty,
        "id" -> getId(ty),
        "brandId" -> getBrandId(ty),
        "value" -> getValue,
        "cityCd" -> "city01",
        "input" -> getInput
      )
      val msg = JsonUtil.toJacksonString(map)
      println(s"${msg}")
      val data = new KeyedMessage[String, String]("hotWords", msg)
      producer.send(data)
      count += 1
      if (count > 10) return
    }
    producer.close()
  }

  def getInput: String = {
    Random.shuffle(input).head
  }

  def getValue: String = {
    Random.shuffle(values).head
  }

  def getUserId: String = {
    //    s"user_${Random.nextInt(10000)}"
    s"user_1"
  }

  def getType: String = {
    Random.shuffle(types).head
  }

  def getId(`type`: String): String = {
    val id = Random.nextInt(1000)
    `type` match {
      case "01" => Random.shuffle(immutable.Seq("5", s"brand_${id}")).headOption getOrElse "5"
      case "02" => s"area_${id}"
      case "03" => s"landmark_${id}"
      case "04" => Random.shuffle(immutable.Seq("5", s"shop_${id}")).headOption getOrElse "5"
      case _ => ""
    }
  }

  def getBrandId(`type`: String): String = {
    val id = Random.nextInt(1000)
    `type` match {
      case "01" | "04" => Random.shuffle(immutable.Seq("5", s"brand_${id}")).headOption getOrElse "5"
      case _ => "5"
    }
  }
}
