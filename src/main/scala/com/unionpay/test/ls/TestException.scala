package com.unionpay.test.ls

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/11/9.
  */

object TestException {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("InsertData to CMS === 插入MongoShop数据")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val datas = sc.textFile("")
    sc.stop()
  }
}