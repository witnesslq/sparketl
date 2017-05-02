package com.unionpay.test

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ywp on 2016/6/24.
  */
object SparkHive {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("SparkHive")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val df = Seq((1, Seq("hello", "sb"), Map("name" -> "a")), (2, Seq("world", "jb"), Map("name" -> "b"))).toDF("id", "name", "st")
    df.printSchema()
    val fields = df.schema.fieldNames
    val head = fields.mkString("\t")
    val result = df.mapPartitions(it => {
      it.map(row => {
        row.mkString("\t")
      })
    }).collect()
    println(head)
    result.foreach(println)
    sc.stop()
  }

}


