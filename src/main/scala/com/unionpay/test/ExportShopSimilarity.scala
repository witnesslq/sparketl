package com.unionpay.test

import java.io.PrintWriter

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ywp on 2016/8/10.
  */
object ExportShopSimilarity {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("ExportShopSimilarity")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.yarn.driver.memoryOverhead", "1024")
      .set("spark.yarn.executor.memoryOverhead", "2000")
      .set("spark.driver.maxResultSize", "10g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)
    //    union
    crawl
    //    group
    sc.stop()
  }


  def group(implicit sqlContext: SQLContext): Unit = {
    import sqlContext.implicits._
    val simDF = sqlContext.read.parquet("/unionpay/shop/unioncrawlsimilarity_test")
      .filter(!$"sim".isNaN)
      .filter("sim=1")

    val idDF = simDF.groupBy("unionShopNo").agg(collect_set("crawlShopNo").as("cc")).filter("size(cc)>1").selectExpr("unionShopNo")

    val tmpSimDF = simDF.as('a)
      .join(idDF.as("b"), $"a.unionShopNo" === $"b.unionShopNo", "leftsemi")
      .selectExpr("a.*")

    val pw = new PrintWriter("/tmp/group.csv", "UTF-8")
    pw.println("相似度,字符串算法,店铺1id,品牌名,店铺名,分词,店铺2id,店铺名,分词")


    tmpSimDF.mapPartitions(it => {
      it.map(row => {
        val sim = row.getAs[Double]("sim")
        val rate = row.getAs[Double]("rate")
        val shopNo = row.getAs[String]("unionShopNo")
        val brandName = row.getAs[String]("unionBrandName")
        val shopName = row.getAs[String]("unionShopName")
        val name = row.getAs[Seq[String]]("unionSpName")
        val otherNo = row.getAs[String]("crawlShopNo")
        val otherBrand = row.getAs[String]("crawlShopName")
        val otherName = row.getAs[Seq[String]]("crawlSpName")
        s"${sim},\t${rate},\t${shopNo},\t${brandName},\t${shopName},\t${name.mkString("[", "|", "]")},\t${otherNo},\t${otherBrand},\t${otherName.mkString("[", "|", "]")}"
      })
    })
      .collect()
      .foreach(pw.println)

    pw.close()
  }

  def crawl(implicit sqlContext: SQLContext): Unit = {
    import sqlContext.implicits._
    val simDF = sqlContext.read.parquet("/unionpay/shop/unioncrawlsimilarity_test")
      .filter(!$"sim".isNaN)
      .filter("sim<1 and rate>0.8 and rate<1")
      .orderBy($"rate".desc)
    val pw = new PrintWriter("/tmp/sim_crawl_shop.csv", "UTF-8")
    pw.println("相似度,字符串算法,店铺1id,品牌名,店铺名,分词,店铺2id,店铺名,分词")


    simDF.mapPartitions(it => {
      it.map(row => {
        val sim = row.getAs[Double]("sim")
        val rate = row.getAs[Double]("rate")
        val shopNo = row.getAs[String]("unionShopNo")
        val brandName = row.getAs[String]("unionBrandName")
        val shopName = row.getAs[String]("unionShopName")
        val name = row.getAs[Seq[String]]("unionSpName")
        val otherNo = row.getAs[String]("crawlShopNo")
        val otherBrand = row.getAs[String]("crawlShopName")
        val otherName = row.getAs[Seq[String]]("crawlSpName")
        s"${sim},\t${rate},\t${shopNo},\t${brandName},\t${shopName},\t${name.mkString("[", "|", "]")},\t${otherNo},\t${otherBrand},\t${otherName.mkString("[", "|", "]")}"
      })
    })
      .collect()
      .foreach(pw.println)

    pw.close()
  }

  def union(implicit sqlContext: SQLContext): Unit = {
    import sqlContext.implicits._
    val simDF = sqlContext.read.parquet("/unionpay/shop/similarity")
      .filter(!$"sim".isNaN)
      .filter("sim>0.98")
      .orderBy($"sim".desc)
    val pw = new PrintWriter("/tmp/sim_shop.csv", "UTF-8")
    pw.println("相似度,店铺1id,品牌名,店铺名,分词,店铺2id,品牌,店铺名,分词")

    simDF.mapPartitions(it => {
      it.map(row => {
        val sim = row.getAs[Double]("sim")
        val shopNo = row.getAs[String]("shopNo")
        val brandName = row.getAs[String]("brandName")
        val shopName = row.getAs[String]("shopName")
        val name = row.getAs[Seq[String]]("name")
        val otherNo = row.getAs[String]("otherNo")
        val otherBrand = row.getAs[String]("otherBrand")
        val otherShop = row.getAs[String]("otherShop")
        val otherName = row.getAs[Seq[String]]("otherName")
        s"${sim},\t${shopNo},\t${brandName},\t${shopName},\t${name.mkString("[", "|", "]")},\t${otherNo},\t${otherBrand},\t${otherShop},\t${otherName.mkString("[", "|", "]")}"
      })
    })
      .collect()
      .foreach(pw.println)

    pw.close()
  }
}
