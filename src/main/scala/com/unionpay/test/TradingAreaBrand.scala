package com.unionpay.test

import java.io.PrintWriter
import java.sql.Timestamp

import com.unionpay.db.jdbc.JdbcUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.unionpay.db.mongo.MongoUtil._
import com.unionpay.util.IdGenerator
import org.apache.spark.sql.functions._
import org.joda.time.LocalDateTime
import org.joda.time.format.DateTimeFormat
/**
  * Created by ywp on 2016/8/11.
  */
object TradingAreaBrand {

  private lazy val timeStampFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  private lazy val currentTimestamp = Timestamp.valueOf(LocalDateTime.now().toString(timeStampFormatter))


  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("TradingArea")
      //      .setMaster("local[*]")
      .set("spark.kryoserializer.buffer.max", "1024m")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.udf.register("getAreaNo", () => IdGenerator.generateAreaID)
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")

    implicit val df = sqlContext.mongoDF("tradingArea")
      .selectExpr("getAreaNo() area_no", "trim(tradingAreaName) area_nm",
        "provinceCode prov_cd", "cityCode city_cd", "countyCode county_cd"
      )
      .withColumn("ROW_CRT_USR", lit(""))
      .withColumn("REC_UPD_USR", lit(""))
      .withColumn("ROW_CRT_TS", lit(currentTimestamp))
      .withColumn("REC_UPD_TS", lit(currentTimestamp))

    JdbcUtil.save2Mysql("tbl_content_busi_area")

    sc.stop()
  }

  def brand(implicit sqlContext: SQLContext): Unit = {
    val ub = "/tmp/ub.csv"
    val pw1 = new PrintWriter(ub, "UTF-8")
    val uBrandDF = JdbcUtil.mysqlJdbcDF("tbl_content_brand_inf", "sink")
      .selectExpr("brand_no", "brand_nm")
    val ubc = uBrandDF.map(r => (r.getAs[String]("brand_no"), r.getAs[String]("brand_nm"))).collect()
    ubc.foreach(x => pw1.println(s"${x._1},${x._2}"))
    pw1.close()


    val mb = "/tmp/mb.csv"
    val pw2 = new PrintWriter(mb, "UTF-8")
    val mBranDF = sqlContext.mongoDF("brand")
      .selectExpr("_id.oid oid", "name")
    val mbc = mBranDF.map(r => (r.getAs[String]("oid"), r.getAs[String]("name"))).collect()
    mbc.foreach(x => pw2.println(s"${x._1},${x._2}"))
    pw2.close()


    val bunion = "/tmp/b_union.csv"
    val pw3 = new PrintWriter(bunion, "UTF-8")
    val ubb = ubc.map(_._2.trim).union(mbc.map(_._2.trim)).distinct
    ubb.foreach(pw3.println)
    pw3.close()
  }

  def area(implicit sqlContext: SQLContext): Unit = {
    val mAreaDF = sqlContext.mongoDF("tradingArea")
      .selectExpr("_id.oid oid", "tradingAreaName")
    val uAreaDF = JdbcUtil.mysqlJdbcDF("tbl_content_busi_area", "sink").selectExpr("area_no", "area_nm")
    val ma = mAreaDF
      .map(r => (r.getAs[String]("oid"), r.getAs[String]("tradingAreaName")))
      .collect()
    val mar = "/tmp/ma.csv"
    val pwm = new PrintWriter(mar, "UTF-8")
    ma.foreach(pwm.println)
    pwm.close()
    val ua = uAreaDF.map(r => (r.getAs[String]("area_no"), r.getAs[String]("area_nm")))
      .collect()
    val uar = "/tmp/ua.csv"
    val pwu = new PrintWriter(uar, "UTF-8")
    ua.foreach(pwu.println)
    pwu.close()
    val unionAreaNames = ma.map(_._2.trim).union(ua.map(_._2.trim)).distinct
    val aunion = "/tmp/a_union.csv"
    val pw3 = new PrintWriter(aunion, "UTF-8")
    unionAreaNames.foreach(pw3.println)
    pw3.close()
  }
}
