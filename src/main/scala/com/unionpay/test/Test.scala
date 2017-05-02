package com.unionpay.test

import com.unionpay.db.mongo.MongoUtil._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ywp on 2016/8/21.
  */
object Test {


  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Test")
      .setMaster("local[*]")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")

    val mongoDF = sqlContext.fetchCrawlShopDF

    val mongoCt = mongoDF
      .selectExpr("case when size(subCategory)=0 then '' else trim(subCategory[0]) end subCategory")
      .map(_.getAs[String]("subCategory"))
      .collect()
      .distinct

    /*val shopDF = mysqlJdbcDF("tbl_content_shop_inf", "sink")
      .selectExpr("shop_no shopId", "shop_nm shopName", "brand_no brandId").filter("trim(brandId)!=''")
      .repartition(12, $"brandId")

    val brandDF = mysqlJdbcDF("tbl_content_brand_inf", "sink").selectExpr("brand_no brandId", "brand_nm brandName")
      .repartition(12, $"brandId")

    val brandHotDF = shopDF.as('a)
      .join(brandDF.as('b), $"a.brandId" === $"b.brandId")
      .selectExpr("a.brandId", "a.shopId", "a.shopName", "b.brandName")

    brandHotDF.coalesce(12).write.mode(SaveMode.Overwrite).parquet("/hotword/shopbrand")*/

    val df = Seq((1, 1.222D)).toDF("id", "score")

    df.selectExpr("id", "round(score,2) score")
      .show()
    sc.stop()

  }


}
