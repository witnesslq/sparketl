package com.unionpay.test.ls

import org.apache.spark.{SparkConf, SparkContext}
import com.unionpay.db.jdbc.JdbcUtil._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

/**
  * Created by ls on 2016/9/28.
  */
object TmpDistinct {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("TmpTableDistinct---数据分割")
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

    val TDF = mysqlJdbcDF("TBL_TTTT1", "sink")


    val delDF = mysqlJdbcDF("tbl_tmp5", "sink")

    val doubleDF = delDF.filter(length($"ENTRY_INS_ID_CD") > 17)
    val onlyDF = delDF.filter(length($"ENTRY_INS_ID_CD") <= 17)

    val tmpDF = doubleDF.as('a)
      .join(TDF.as('b), $"a.THIRD_PARTY_INS_ID" === $"b.THIRD_PARTY_INS_ID", "left_outer")
      .selectExpr("trim(a.THIRD_PARTY_INS_ID) THIRD_PARTY_INS_ID", "trim(b.ENTRY_INS_ID_CD) ENTRY_INS_ID_CD")
    println(tmpDF.count())

    val df = onlyDF.unionAll(tmpDF)

    save2Mysql("tbl_distinct_tmp")(df)

    sc.stop()
  }

}
