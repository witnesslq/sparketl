package com.unionpay.test.online

import com.unionpay.db.jdbc.JdbcUtil
import com.unionpay.db.mongo.MongoUtil._
import com.unionpay.util.IdGenerator
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/12/1.
  */
object MongoTest {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("mongo Brand === 处理mongo品牌数据")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "32")

    val shopBrandDF = sqlContext.read.parquet("/tmp/ls/shop_brand_mongo")
      .selectExpr("src_shop_no", "brand_nm")

    val brandUnionDF = sqlContext.read.parquet("/tmp/ls/tbl_content_brand_inf1205")
      .filter($"src_brand_tp" === "1")
      .selectExpr("brand_no", "brand_nm")

    val brandCrawlDF = JdbcUtil.mysqlJdbcDF("tbl_content_brand_inf_20161201", "sink")
      .selectExpr("brand_no", "brand_nm")

    val df1 = shopBrandDF.as('a)
      .join(brandUnionDF.as('b), $"a.brand_nm" === $"b.brand_nm")
      .selectExpr("a.src_shop_no src_shop_no", "b.brand_no")
      .groupBy("src_shop_no")
      .agg(first("brand_no").as("brand_no"))

    val df2 = shopBrandDF.as('a)
      .join(brandCrawlDF.as('b), $"a.brand_nm" === $"b.brand_nm")
      .selectExpr("a.src_shop_no src_shop_no", "b.brand_no")
      .groupBy("src_shop_no")
      .agg(first("brand_no").as("brand_no"))

    df1.unionAll(df2).repartition(4).write.mode(SaveMode.Overwrite).parquet("/tmp/ls/update_brand_no_20161206")

    sc.stop()
  }
}
