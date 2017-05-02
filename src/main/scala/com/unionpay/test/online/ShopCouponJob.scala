package com.unionpay.test.online

import com.unionpay._
import com.unionpay.etl._
import com.unionpay.db.mongo.MongoUtil._
import com.unionpay.db.jdbc.{JdbcSaveMode, JdbcUtil}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/12/1.
  */
object ShopCouponJob {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("ShopCouponRelationJob--商户优惠关系任务")
      //      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.yarn.driver.memoryOverhead", "1024")
      .set("spark.yarn.executor.memoryOverhead", "2000")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)



    sc.stop()
  }
}
