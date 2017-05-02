package com.unionpay.test.ls

import com.unionpay.db.jdbc.MysqlConnection
import com.unionpay.es._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

/**
  * Created by ls on 2017/3/1.
  */
object ShopLocalESJob {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("插入ES")
      .setMaster("local[*]")
      .build("content", "shop", Option("shopId"))
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")

    val mysql = MysqlConnection.build("mysql2mysql", "sink")
    import mysql._

    val shopDF = sqlContext.jdbcDF("shop")
      .map(r => {

        val shopId = r.getAs[String]("shop_id")
        val shopName = r.getAs[String]("shop_name")
        val brandName = r.getAs[String]("brand_name")
        val shopLat = r.getAs[String]("shop_lat").toDouble
        val shopLng = r.getAs[String]("shop_lnt").toDouble

        val location = Location(shopLat, shopLng)

        (shopId, shopName, brandName, shopLat, shopLng, location)
      }).toDF("shopId", "shopName", "brandName", "lat", "lng", "location")

    shopDF.saveToEs(sqlContext.sparkContext.getConf.getAll.toMap)

    sc.stop()
  }
}
