package com.unionpay.etl

import java.sql.Timestamp

import com.unionpay.db.jdbc.{JdbcUtil, MysqlConnection}
import com.unionpay.util.{ConfigUtil, IdGenerator}
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.LocalDateTime
import org.joda.time.format.DateTimeFormat
import com.unionpay.db.mongo.MongoUtil._

/**
  * Created by ywp on 2016/7/7.
  */
object BizAreaJob {


  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("BizAreaJob--商区任务")
      //      .setMaster("local[4]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")

    //城市码的处理
    sqlContext.udf.register("city", (cityCode: String) => {
      cityCode match {
        case cd: String if cd.startsWith("110") => "110000"
        case cd: String if cd.startsWith("120") => "120000"
        case cd: String if cd.startsWith("310") => "310000"
        case cd: String if cd.startsWith("500") => "500000"
        case _ => cityCode
      }
    })
    //银联的商圈表舍弃 直接使用互联网的商圈数据
    sqlContext.udf.register("getAreaNo", () => IdGenerator.generateAreaID)

    implicit val df = sqlContext.mongoDF("tradingArea")
      .selectExpr("trim(tradingAreaName) area_nm", "trim(provinceCode) prov_cd", "city(trim(cityCode)) city_cd", "trim(countyCode) county_cd")
      //如果存在 省市区 以及 商圈名字都一样的 去重
      .distinct()
      .selectExpr("getAreaNo() area_no", "area_nm", "prov_cd", "city_cd", "county_cd")
      .addAlwaysColumn

    JdbcUtil.save2Mysql("tbl_content_busi_area")

    //    etl

    sc.stop()

  }

  def etl(implicit sqlContext: SQLContext) = {
    val bizDistTb = "tbl_chmgm_buss_dist_inf"
    val bizAreaDF = JdbcUtil.mysqlJdbcDF(bizDistTb)
    val config = readConfig.union
    val tmpBizAreaDFDF = bizAreaDF
      .reNameColumn(config.transForm)
      .appendDefaultVColumn(config.default)
      .addAlwaysColumn
    save2Mysql(tmpBizAreaDFDF)
  }

  def readConfig: BizAreaConfig = {
    import net.ceedubs.ficus.Ficus._
    val configName = "bizArea"
    ConfigUtil.readClassPathConfig[BizAreaConfig](configName)
  }

  def save2Mysql(df: DataFrame)(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    val mysql = MysqlConnection.build("mysql2mysql", "sink")
    import mysql._
    val bizAreaTb = "tbl_content_busi_area"
    val areaDF = df.selectExpr("case when AREA_NO=0 then '' else cast(AREA_NO as string) end AREA_NO",
      "AREA_NM", "PROV_CD", "CITY_CD", "COUNTY_CD",
      "AREA_INTRO", "ROW_CRT_USR", "ROW_CRT_TS",
      "REC_UPD_USR", "REC_UPD_TS")
    areaDF.save2DB(bizAreaTb)
  }

}

case class BizArea(transForm: Map[String, String],
                   default: Map[String, String])

case class BizAreaConfig(union: BizArea)
