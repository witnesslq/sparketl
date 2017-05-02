package com.unionpay.test.ls

import com.unionpay.db.jdbc.JdbcUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/9/26.
  */
object GetCouponData {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("ExportShopSimilarity")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.yarn.driver.memoryOverhead", "1024")
      .set("spark.yarn.executor.memoryOverhead", "2000")
      .set("spark.driver.maxResultSize", "10g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")

    val shopDF = JdbcUtil.mysqlJdbcDF("tbl_chmgm_preferential_mchnt_inf")

    val brandDF = JdbcUtil.mysqlJdbcDF("tbl_chmgm_brand_inf")
      .selectExpr("BRAND_ID", "BRAND_NM")

    val couponDF = JdbcUtil.mysqlJdbcDF("TBL_CHMGM_TICKET_COUPON_INF")

    val df = shopDF.as('a)
      .join(brandDF)
  }

}
