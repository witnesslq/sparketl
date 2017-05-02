package com.unionpay.test.ls

import java.sql.{Connection, PreparedStatement}

import com.unionpay.db.jdbc.JdbcUtil._
import com.unionpay.util.IdGenerator
import org.apache.spark.sql.SQLContext
import com.unionpay.db.mongo.MongoUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/9/27.
  */
object Db2Connect {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("sb")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.yarn.driver.memoryOverhead", "1024")
      .set("spark.yarn.executor.memoryOverhead", "2000")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "24")

    val mongoDF = sqlContext.mongoDF("")

  }
}
