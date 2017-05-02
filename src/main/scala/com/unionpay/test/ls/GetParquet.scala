package com.unionpay.test.ls

import com.unionpay.db.jdbc.JdbcUtil
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/9/9.
  */
object GetParquet {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("GetParquest")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    sqlContext.setConf("spark.sql.shuffle.partitions", "6")

    val shopDF = JdbcUtil.mysqlJdbcDF("tbl_chmgm_preferential_mchnt_inf")
      .selectExpr("MCHNT_CD shopId", "MCHNT_NM shopName", "BRAND_ID brandId")

    val brandDF = JdbcUtil.mysqlJdbcDF("tbl_chmgm_brand_inf")
      .selectExpr("BRAND_ID brandId", "BRAND_NM brandName")

    val tmpDF = shopDF.as('a)
      .join(brandDF.as('b), $"a.brandId" === $"b.brandId", "left_outer")
      .selectExpr("a.*", "coalesce(b.brandName) brandName")


    println(tmpDF.count())
    tmpDF.printSchema()
    tmpDF.show()
    val path = "C:/Users/ls/Desktop/parquet"
    tmpDF.write.mode(SaveMode.Overwrite).parquet(path)
    sc.stop()
  }

}
