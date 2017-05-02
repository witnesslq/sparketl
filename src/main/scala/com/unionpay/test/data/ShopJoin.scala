package com.unionpay.test.data

import com.unionpay.test.data.DealArea._
import com.unionpay.test.data.ShopUnionJobTmp._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/10/24.
  */
object ShopJoin {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("InsertData to CMS === 插入数据")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")

    delShop
    delArea

    sc.stop()
  }

}
