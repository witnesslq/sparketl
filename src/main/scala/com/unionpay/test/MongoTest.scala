package com.unionpay.test

import com.unionpay.db.mongo.MongoUtil._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ywp on 2016/8/2.
  */
object MongoTest {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("mongodb test")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    val df = sqlContext.mongoDF("user")
    sc.stop()
  }
}
