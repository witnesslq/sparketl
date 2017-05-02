package com.unionpay.test.ls

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import com.unionpay.db.jdbc.JdbcUtil._
import com.unionpay.db.jdbc._


/**
  * Created by ls on 2016/9/30.
  */
object UpdateInsert {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("IndustryInfoJob--行业类、行业子类任务")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")
    import sqlContext.implicits._

    val df = Seq(
      (1, "pp"),
      (2, "kk"),
      (3, "c"),
      (4, "d"),
      (19, "kkk")
    ).toDF("id", "name")

    saveToMysql("sql_test", JdbcSaveMode.Upsert,"source")(df)


    sc.stop()
  }

}
