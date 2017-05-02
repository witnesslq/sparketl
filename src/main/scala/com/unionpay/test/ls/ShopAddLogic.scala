package com.unionpay.test.ls

import java.math.{BigDecimal => javaBigDecimal}

import com.unionpay.db.jdbc.{JdbcUtil, MysqlConnection}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/9/7.
  */
object ShopAddLogic {

  val serviceMap = Map("免费停车" -> "停车", "无线上网" -> "WIFI支持", "可以刷卡" -> "刷卡")
  val default = "云闪付;免密免签;WIFI支持;停车;刷卡;"
  val ds = default.split(";")
  val x100 = new javaBigDecimal(100)

  private lazy val timeRegex ="""\d{1,2}(:|：)\d{2}""".r
  private lazy val dianRegex ="""\d{1,2}点""".r
  private lazy val chRegex ="""[\u4e00-\u9fa5]""".r

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("shopInfoJob--店铺信息任务")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.yarn.driver.memoryOverhead", "1024")
      .set("spark.yarn.executor.memoryOverhead", "2000")
      .set("spark.network.timeout", "300s")
      //todo 云主机 经常网络超时
      .set("spark.executor.heartbeatInterval", "30s")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")
    etl
    sc.stop()
  }

  def etl(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    //获取营业时间
    sqlContext.udf.register("getShopTime", (time: String) => getShopTime(time))
    //获取停车状态
    sqlContext.udf.register("getServices", (park: String) => getServices(park))
    //获取商店名信息
    sqlContext.udf.register("getShopName", (shopName: String, brandName: String) => getShopName(shopName, brandName))

    //品牌信息
    val brandTb = "TBL_CHMGM_BRAND_INF"
    val brandDF = JdbcUtil.mysqlJdbcDF(brandTb)
      .selectExpr("BRAND_ID", "trim(BRAND_NM) BRAND_NM")


    val shopTb = "tbl_chmgm_preferential_mchnt_inf"
    val shopDF = JdbcUtil.mysqlJdbcDF(shopTb)

    val finalDF = shopDF.as('a)
      .join(broadcast(brandDF).as('b), $"a.BRAND_ID" === $"b.BRAND_ID", "left_outer")
      .selectExpr("a.*", "coalesce(b.BRAND_NM,'') BRAND_NM")

    val exprs = finalDF.schema.fieldNames
      .map(x => {
        x match {
          case "MCHNT_NM" => "getShopName(MCHNT_NM,BRAND_NM) MCHNT_NM"
          case "PARK_INF" => "getServices(PARK_INF) PARK_INF"
          case "BUSS_HOUR" => "getShopTime(BUSS_HOUR) BUSS_HOUR"
          case _ => x
        }
      }).filterNot(_ == "BRAND_NM")


    val df = finalDF.selectExpr(exprs: _*)


    save2Mysql(df)

  }

  def save2Mysql(df: DataFrame)(implicit sqlContext: SQLContext) = {
    val mysql = MysqlConnection.build("mysql2mysql", "source")
    import mysql._
    df.save2DB("tbl_chmgm_shop_inf")
  }

  def getServices(park: String) = {
    Option(park) match {
      case None => "00000"
      case Some(s) => {
        s match {
          case " " | "--" => "00000"
          case x: String if x.contains("无") || x.contains("询") => "00000"
          case _ => "00010"
        }
      }
    }
  }


  def getShopName(shopName: String, brandName: String)(implicit sqlContext: SQLContext) = {
    val spName = shopName.replaceAll("[()（）]", "")
    val spBrand = brandName.replaceAll("[()（）]", "")
    Option(spBrand) match {
      case None => shopName
      case Some(bb) => {
        if (spName.contains(bb)) spName else s"${bb}(${spName})"
      }
      case _ => spName
    }
  }


  def getShopTime(oHour: String) = {

    implicit val timeOrdering = new Ordering[String] {
      val convert = (x: String) => if (x.split(":").head.size < 2) s"0${x}" else x

      override def compare(x: String, y: String): Int = {
        convert(x).compare(convert(y))
      }
    }

    val name = oHour.replaceAll(" ", "").replaceAll("：", ":")
    val finalName = if (name == "24" || name == "二十四小时" || name.contains("24小时")) "00:00-23:59"
    else {
      val timeStr = timeRegex.findAllIn(name).toList
      if (timeStr.isEmpty) {
        val dianStr = dianRegex.findAllIn(name).toList
        if (dianStr.isEmpty || dianStr.size == 1) name
        else {
          dianStr.take(2).map(_.replaceAll("点", "")) match {
            case Nil => name
            case head :: second :: Nil => s"$head:00-${if (second.toInt > 10) second else second.toInt + 12}:00"
          }
        }
      }
      else {
        val mm = timeStr.sorted
        val min = mm.min
        val max = mm.max
        s"$min-$max"
      }
    }
    if (chRegex.findFirstIn(finalName).isDefined) "具体以店内公布为准" else finalName
  }
}