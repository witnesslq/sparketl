package com.unionpay.test.ls

import com.unionpay.db.jdbc.JdbcUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/11/3.
  */
object CalculateData {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("CalculateData---数据统计")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.yarn.driver.memoryOverhead", "1024")
      .set("spark.yarn.executor.memoryOverhead", "2000")
      .set("spark.network.timeout", "300s")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "32")
    import sqlContext.implicits._

    val db2ShopDF = JdbcUtil.mysqlJdbcDF("tbl_chmgm_preferential_mchnt_inf_sb")
    val tmpShopDF = JdbcUtil.mysqlJdbcDF("tbl_chmgm_shop_tmp", "sink")

    val tmpShopDF1 = db2ShopDF.filter(s"date_format(REC_CRT_TS,'yyyy-MM-dd') < '2016-09-01'")
      .selectExpr("MCHNT_CD")
    val tmpShopDF2 = tmpShopDF.selectExpr("MCHNT_CD")
    println("sb中的数据：" + tmpShopDF1.count())
    println("tmp中的数据：" + tmpShopDF2.count())

    val df2_1 = tmpShopDF2.except(tmpShopDF1)
    val df1_2 = tmpShopDF1.except(tmpShopDF2)

    println("tmp比sb多的数据：" + df2_1.count())
    df2_1.map(_.getAs[String]("MCHNT_CD")).collect().foreach(println)
    println("sb比tmp多的数据：" + df1_2.count())
    df1_2.map(_.getAs[String]("MCHNT_CD")).collect().foreach(println)

    val df = tmpShopDF2.intersect(tmpShopDF1)
    println("相同的id个数：" + df.count())

    sc.stop()
  }

}
