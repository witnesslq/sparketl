package com.unionpay.test.data

import com.unionpay.db.jdbc.JdbcUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/11/5.
  */
object ShopTmpJob {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("BrandLogic === 获取品牌逻辑信息")
      //      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "32")

    val shopTmpDF = JdbcUtil.mysqlJdbcDF("tbl_chmgm_shop_tmp_20161103", "sink")
      .drop("transId")

    val brandDF = JdbcUtil.mysqlJdbcDF("tbl_chmgm_brand_tmp", "sink")
      .selectExpr("BRAND_ID", "BRAND_NM")

    val tmpDF = shopTmpDF.as('a)
      .join(brandDF.as('b), $"a.NEW_BRAND" === $"b.BRAND_NM", "left_outer")
      .selectExpr("a.*", "coalesce(b.BRAND_ID,a.BRAND_ID) transId")

    JdbcUtil.save2Mysql("tbl_chmgm_shop_tmp")(tmpDF)

    sc.stop()
  }
}
