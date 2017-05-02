package com.unionpay.test.ls

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.unionpay._

/**
  * Created by ls on 2016/10/8.
  */
object TestTime {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("ShopInfoRestfulJob--店铺信息任务")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2000m")
      .set("spark.yarn.driver.memoryOverhead", "2048")
      .set("spark.yarn.executor.memoryOverhead", "2000")
      .set("spark.network.timeout", "300s")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")
    import sqlContext.implicits._


    val df = Seq(
      (1, "ls", "2016-10-08 10:00:00"),
      (2, "ll", "2016-10-04 10:00:00"),
      (3, "lk", "2016-10-05 10:00:00"),
      (4, "lp", "2016-10-06 10:00:00"),
      (5, "lq", "2016-10-07 10:00:00")
    ).toDF("id", "name", "rec_upd_ts")

    val dateTime = "2016-10-01 11:00:00"


    sc.stop()

  }

}
