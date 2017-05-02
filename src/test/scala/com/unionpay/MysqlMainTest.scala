package com.unionpay

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import com.unionpay.db.jdbc.JdbcUtil

/**
  * Created by ywp on 2016/6/7.
  */
object MysqlMainTest {

  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setAppName("mysql test")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    /*insert
    read*/
    import sqlContext.implicits._

    val df = (0 to 100000).map(_.toString).map(x => (x, s"{$x:$x}")).toDF("id", "json")

    df.write.mode(SaveMode.Overwrite).parquet("D:\\mockData")

    sc.stop()
  }

  def read(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    val df = JdbcUtil.mysqlJdbcDF("test")
    println(s"分区数:${df.rdd.getNumPartitions}")
    df.printSchema()
    df.show()
    df.select(count($"name")).show()
  }

  def insert(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    implicit val user = sqlContext.sparkContext.parallelize(0.to(10).map(_.toString)).toDF("NAME")
    JdbcUtil.save2Mysql("test")
  }

}
