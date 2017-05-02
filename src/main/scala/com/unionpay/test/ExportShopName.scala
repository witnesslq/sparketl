package com.unionpay.test

import java.io.PrintWriter

import com.unionpay.db.jdbc.JdbcUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ywp on 2016/8/10.
  */
object ExportShopName {


  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("ExportShopName")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Stream[String]]))
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "4")
    import sqlContext.implicits._

    val shopDF = mysqlJdbcDF("tbl_chmgm_preferential_mchnt_inf")
      .selectExpr("trim(MCHNT_NM) MCHNT_NM", "BRAND_ID")
      .filter("MCHNT_NM='利群城阳购物广场（利群家电）'")
    val brandDF = mysqlJdbcDF("tbl_chmgm_brand_inf").selectExpr("BRAND_ID", "trim(BRAND_NM) BRAND_NM")
      .filter("BRAND_NM='利群集团'")

    val broadCastBrand = sc.broadcast[Stream[String]](brandDF.selectExpr("BRAND_NM").distinct().map(_.getAs[String]("BRAND_NM")).collect().toStream)
    val brandIt = udf[String, String, String]((shopName: String, brandName: String) => {
      Option(brandName) match {
        case None => shopName
        case Some(bb) => {
          val brands = broadCastBrand.value
          val shop = if (brands.find(x => shopName.contains(x)).isDefined) shopName else s"${bb}(${shopName})"
          println(shop, shopName, bb)
          shop
        }
      }
    })

    val shopBrandDF = shopDF.as('a)
      .join(brandDF.as('b), $"a.BRAND_ID" === $"b.BRAND_ID", "left_outer")
      .selectExpr("a.MCHNT_NM", "b.BRAND_NM")
      .select($"BRAND_NM".as("brandName"), brandIt(regexp_replace($"MCHNT_NM","""[（）()]""", ""), $"BRAND_NM").as("shopName"))
      .show(10000)

    sc.stop()
  }

}
