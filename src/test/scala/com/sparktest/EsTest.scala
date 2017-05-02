package com.sparktest

import org.scalatest.FunSuite
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._
import com.unionpay.es._
import org.apache.spark.sql.functions._

/**
  * Created by ls on 2016/11/30.
  */
class EsTest extends FunSuite {

  test("es") {

    val conf = new SparkConf()
      .setAppName("ShopInfo2EsJob--商户落地elasticsearch任务")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.yarn.driver.memoryOverhead", "1024")
      .set("spark.yarn.executor.memoryOverhead", "2000")
      //      .set("spark.speculation", "true")
      .set("spark.network.timeout", "300s")
      //todo 云主机 经常网络超时
      .set("spark.executor.heartbeatInterval", "50s")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .build("test", "test", Option("shopId"))
      .registerKryoClasses(Array(classOf[PlayLoadCount], classOf[BrandSuggest]))
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "4")

    val shopDF = Seq((1, "s1", "1", "1"), (2, "s2", "1", "0"), (3, "s3", "1", ""),(4, "s3", "2", " "))
      .toDF("shopId", "shopName", "brandId", "shopValidSt")

    val brandDF = Seq(("1", "b1"), ("2", "b2"), ("3", "b3"))
      .toDF("brandId", "brandName")

    val tmpBrandShopDF = shopDF.filter("shopValidSt='1'").selectExpr("shopId", "brandId")
      .groupBy("brandId").agg(countDistinct("shopId").as("shopCount"))

    val brandShopDF = brandDF.as("a")
      .join(tmpBrandShopDF.as('b), $"a.brandId" === $"b.brandId", "left_outer")
      .selectExpr("a.brandId", "a.brandName", "coalesce(b.shopCount,0) shopCount")

    val tDF = shopDF.as('a)
      .join(brandShopDF.as('c), $"a.brandId" === $"c.brandId", "left_outer")
      .selectExpr("a.*", "coalesce(c.shopCount, 0) shopCount", "coalesce(c.brandName, '') brandName")
      .map(r => {
        val shopValidSt = r.getAs[String]("shopValidSt")
        val shopId = r.getAs[Int]("shopId")
        val brandId = r.getAs[String]("brandId")
        val shopName = r.getAs[String]("shopName")
        val brandName = r.getAs[String]("brandName")
        val shopCount = r.getAs[Long]("shopCount")
        val pl = if (shopValidSt == "1") PlayLoadCount("01", brandId, shopCount) else PlayLoadCount("", "", 0L)
        val bs = if (shopValidSt == "1") BrandSuggest(Seq(brandName), brandName, pl) else BrandSuggest(Seq(""), "", pl)
        (shopId, shopName, bs)
      }).toDF("shopId", "shopName", "b_suggest")

    tDF.saveToEs(sqlContext.sparkContext.getConf.getAll.toMap)
  }
}

case class BrandSuggest(input: Seq[String], output: String, payload: PlayLoadCount, weight: Option[Int] = None)

case class PlayLoadCount(`type`: String, id: String, shopCount: Long)
