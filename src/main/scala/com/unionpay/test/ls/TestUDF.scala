package com.unionpay.test.ls

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.unionpay.db.mongo.MongoUtil._
import com.unionpay.util.ConfigUtil
import com.unionpay.etl._

import net.ceedubs.ficus.readers.ArbitraryTypeReader._

/**
  * Created by ls on 2016/9/6.
  */
object TestUDF {
  val serviceMap = Map("免费停车" -> "停车", "无线上网" -> "WIFI支持", "可以刷卡" -> "刷卡")
  val default = "云闪付;免密免签;WIFI支持;停车;刷卡;"
  val ds = default.split(";")


  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("testUDF")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)

    //    sqlContext.udf.register("shopService", (tags: Seq[String]) => shopService(tags))

    etl
    sc.stop()
  }

  def etl(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    val config = readConfig.crawl
    val mongoDF = sqlContext.fetchCrawlShopDF
      .reNameColumn(config.reName)
      .appendDefaultVColumn(config.default)
      .filter(!($"hours" === ""))
      //        .selectExpr("hours")
      .selectExpr("regexp_replace(hours,'：',':') busi_time")
      .distinct()
      .limit(20)

    //        .selectExpr("")
    //      .selectExpr("case when length(openingHours)>60 then substring(openingHours,0,60) else openingHours end busi_time")

    mongoDF.printSchema()
    mongoDF.show(20)
  }

  def readConfig: ShopConfig = {
    import net.ceedubs.ficus.Ficus._
    val configName = "shopInfo"
    ConfigUtil.readClassPathConfig[ShopConfig](configName)
  }

}

case class Shop(reName: Map[String, String],
                default: Map[String, String]
               )

case class ShopConfig(crawl: Shop)

