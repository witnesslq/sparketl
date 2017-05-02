package com.unionpay.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming._

/**
  * Created by ywp on 2016/9/5.
  */
object StreamingWindowTest {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("windowTest")

    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.sparkContext.setLogLevel("ERROR")
    //    ssc.checkpoint("D:\\window_text")
    ssc.checkpoint("/tmp/checkpoint_test")

    val lines = ssc.socketTextStream("cdh1", 3333)

    val words = lines.flatMap(_.split(" ")).map((_, 1))


    words.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(10), Seconds(10))


    //    words.reduceByKeyAndWindow(_ + _, _ - _, Seconds(30), Seconds(10))
    //      .print()


    val stateSpec = StateSpec.function(trackStateFunc _)
      .numPartitions(4)
      .timeout(Seconds(60))

    val res = words.mapWithState(stateSpec)

    res.stateSnapshots().print()


    ssc.start()
    ssc.awaitTermination()

  }

  def trackStateFunc(batchTime: Time, key: String, value: Option[Int], state: State[Long]): Option[(String, Long)] = {
    val sum = value.getOrElse(0).toLong + state.getOption.getOrElse(0L)
    val output = (key, sum)
    state.update(sum)
    Some(output)
  }

}
