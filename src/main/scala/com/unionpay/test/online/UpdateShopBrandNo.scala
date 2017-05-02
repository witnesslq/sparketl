package com.unionpay.test.online

import com.unionpay.db.jdbc.{JdbcSaveMode, JdbcUtil}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/11/29.
  */
object UpdateShopBrandNo {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("updateBrandNo")
      //      .setMaster("local[*]")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")

    sqlContext.udf.register("bn", (o: String, n: String) => {
      val on = o.trim.replaceAll(" ", "")
      val nn = n.trim.replaceAll(" ", "")
      if (nn.isEmpty) on else nn
    })

    val shopBrandDF = sqlContext.read.parquet("/tmp/ls/result_tag")
      .selectExpr("MCHNT_CD src_shop_no", "bn(brandName,NEW_Brand) brandName")

    val shopDF = JdbcUtil.mysqlJdbcDF("tbl_content_shop_inf_20161129", "sink")
      .selectExpr("src_shop_no", "brand_no")

    val brandDF = JdbcUtil.mysqlJdbcDF("tbl_content_brand_inf_29", "sink")
      .selectExpr("brand_no", "brand_nm")

    val updateDF = shopBrandDF.as('a)
      .join(brandDF.as('b), $"a.brandName" === $"b.brand_nm")
      .selectExpr("a.src_shop_no", "b.brand_no")

    val finalDF = shopDF.as('a)
      .join(updateDF.as('b), $"a.src_shop_no" === $"b.src_shop_no", "left_outer")
      .selectExpr("a.src_shop_no", "coalesce(b.brand_no,a.brand_no) brand_no")

    JdbcUtil.saveToMysql("tbl_content_shop_inf_20161129", JdbcSaveMode.Upsert)(finalDF)

    sc.stop()
  }

}
