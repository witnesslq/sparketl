package com.unionpay.test.ls

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/11/28.
  */
object ParquetCompare {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("生成shopCouponSql语句")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "4")
    import sqlContext.implicits._
    val p1 = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\result")
    val p2 = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\result_new")
    val p3 = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\result_new_1")

    println(p1.count())
    println(p2.count())
    println(p3.count())

    sc.stop()
  }

}
