package com.unionpay

import com.unionpay.db.jdbc.{Db2Connection, MysqlConnection}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ywp on 2016/6/7.
  */
object Db2MainTest {

  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setAppName("db2 test")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)

    //    insert
    read

    sc.stop()
  }

  def read(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    val db2 = Db2Connection.build("db2mysql", "source")
    import db2._
    val df = sqlContext.jdbcDF("user")
    df.printSchema()
    df.select($"NAME".cast(IntegerType))
      .printSchema()

  }

  def insert(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    val mysql = MysqlConnection.build("db2mysql", "sink")
    import mysql._
    val datas = sqlContext.sparkContext.parallelize(0.to(10))
    val user = datas.map(s => (s, s.toString)).toDF("age", "name")
    user.save2DB("user")
  }


}
