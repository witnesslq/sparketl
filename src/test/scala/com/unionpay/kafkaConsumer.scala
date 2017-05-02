//
//package com.unionpay
//
//import java.util.Properties
//import java.util.concurrent.Executors
//
//import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
//
//import scala.collection.JavaConversions._
//
///**
//  * Created by ywp on 2016/6/7.
//  */
//object kafkaConsumer {
//
//  def main(args: Array[String]) {
//
//    //    Producer.produce("test")
//    //    Consumer.consume("test")
//
//    val pool = Executors.newFixedThreadPool(2)
//    pool.submit(new Consumer("test"))
//    pool.submit(new Producer("test"))
//    pool.shutdown()
//
//  }
//
//}
//
//object Producer {
//  def produce(topic: String) = {
//    new Producer(topic).run()
//  }
//}
//
//class Producer(topic: String) extends Runnable {
//  val props = new Properties()
//  props.put("bootstrap.servers", "localhost:9092")
//  props.put("client.id", "DemoProducer")
//  props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
//  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//  private lazy val producer = new KafkaProducer[Int, String](props)
//
//  override def run(): Unit = {
//    var messageNo = 0
//    println("start producing")
//    while (true) {
//      val messageStr = "Message_" + messageNo
//      try {
//        val record = new ProducerRecord(topic, messageNo, messageStr)
//        producer.send(record)
//        println("Sent message: (" + messageNo + ", " + messageStr + ")")
//        if (messageNo > 10) return
//      } catch {
//        case e: Exception =>
//          e.printStackTrace()
//      }
//      messageNo += 1
//    }
//  }
//
//}
//
//object Consumer {
//  def consume(topic: String) = {
//    new Consumer(topic).run()
//  }
//}
//
//class Consumer(topic: String) extends Runnable {
//  val props = new Properties()
//  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
//  props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer")
//  props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
//  props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
//  props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
//  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer")
//  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
//
//  val consumer = new KafkaConsumer(props)
//
//  override def run(): Unit = {
//    try {
//      println(s"start consuming!!!")
//      consumer.subscribe(Iterable(topic))
//      while (true) {
//        val records = consumer.poll(1000)
//        for (record <- records) {
//          println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
//        }
//      }
//      //应该与客户端的操作一起组成原子性操作
//      consumer.commitSync()
//    } catch {
//      case e: Exception => throw e
//    } finally {
//      println("stopping")
//      consumer.close()
//    }
//  }
//}
//
