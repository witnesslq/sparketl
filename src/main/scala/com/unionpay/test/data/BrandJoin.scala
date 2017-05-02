package com.unionpay.test.data

import com.unionpay.db.jdbc.JdbcUtil
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/10/20.
  */
object BrandJoin {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("InsertData to CMS === 插入数据")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")

    val brandNewDF = JdbcUtil.mysqlJdbcDF("tbl_chmgm_brand_new_inf", "sink")
    val logicDF = JdbcUtil.mysqlJdbcDF("tbl_brand_etl_logic", "sink")

    val df = brandNewDF.as('a)
      .join(logicDF.as('b), $"a.BRAND_NM" === $"b.brand_new_name", "left_outer")
      .selectExpr("b.brand_no BRAND_NO")
      .distinct()

    val brandImgTb = "TBL_CHMGM_BRAND_PIC_INF"
    val brandImgDF = JdbcUtil.mysqlJdbcDF(brandImgTb)
      .filter($"CLIENT_TP" === lit("2"))
      .withColumnRenamed("BRAND_ID", "BRAND_NO")
      .selectExpr("SEQ_ID", "BRAND_NO")

    val imageDF = brandImgDF.join(df, brandImgDF("BRAND_NO") === df("BRAND_NO"), "left_outer")
      .selectExpr("SEQ_ID")

    imageDF.show(10)
    println(imageDF.count())

    sc.stop()

  }

}
