package com.unionpay.test.data

import com.unionpay.db.jdbc.{JdbcSaveMode, JdbcUtil}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.sys.process._

/**
  * Created by ls on 2016/10/20.
  */
object DealBrand {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("InsertData to CMS === 插入数据")
      //      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "32")
    import sqlContext.implicits._


    val shopDF = JdbcUtil.mysqlJdbcDF("tbl_content_shop_inf", "sink")
      .selectExpr("src_shop_no", "brand_no")

    //    shopDF.repartition(30).write.mode(SaveMode.Overwrite).parquet("/tmp/ls/shop")

    val brandDF = JdbcUtil.mysqlJdbcDF("tbl_content_brand_inf", "sink")
      .selectExpr("brand_no", "src_brand_no")

    val df = shopDF.as('a)
      .join(brandDF.as('b), $"a.brand_no" === $"b.src_brand_no", "left_outer")
      .selectExpr("a.src_shop_no src_shop_no", "coalesce(b.brand_no,'') brand_no")

    JdbcUtil.saveToMysql("tbl_content_shop_inf", JdbcSaveMode.Upsert)(df)

    sc.stop()
  }

}
