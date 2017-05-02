package com.unionpay.test.ls

import java.io.PrintWriter

import com.unionpay.db.jdbc.JdbcUtil
import com.unionpay.mllib.GeoLocation
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by ls on 2016/9/14.
  */
object SimTest {


  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("SimTest--相似度")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.yarn.driver.memoryOverhead", "1024")
      .set("spark.yarn.executor.memoryOverhead", "2000")
      .registerKryoClasses(Array(classOf[GeoLocation]))

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)

    sqlContext.setConf("spark.sql.shuffle.partitions", "6")

    val df = JdbcUtil.mysqlJdbcDF("tbl_content_shop_inf")

    sc.stop()

  }

}
