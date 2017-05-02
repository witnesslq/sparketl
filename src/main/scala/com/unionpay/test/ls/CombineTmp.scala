package com.unionpay.test.ls

import com.unionpay.db.jdbc.{JdbcSaveMode, JdbcUtil}
import com.unionpay.db.jdbc.JdbcUtil._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/9/28.
  */
object CombineTmp {

  def main(args: Array[String]) {


    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("CompanySplit---数据分割")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.yarn.driver.memoryOverhead", "1024")
      .set("spark.yarn.executor.memoryOverhead", "2000")
      .set("spark.network.timeout", "300s")
      //todo 云主机 经常网络超时
      .set("spark.executor.heartbeatInterval", "30s")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")

    val df = Seq(
      (1, "lpppo"),
      (2, "lrppr"),
      (3, "lypyy"),
      (4, "loppo"),
      (5, "lippp"),
      (6, "lpppp")
    ).toDF("ID", "small")

    JdbcUtil.saveToMysql("SMALL", JdbcSaveMode.Upsert, "source")(df)

    val small = mysqlJdbcDF("SMALL")
      .selectExpr("small")

    small.printSchema()
    small.show()


    /*  small.printSchema()
      small.show()
      val big = mysqlJdbcDF("big")
        .selectExpr("BIG")
      big.printSchema()
      big.show()*/

    sc.stop()
  }

}
