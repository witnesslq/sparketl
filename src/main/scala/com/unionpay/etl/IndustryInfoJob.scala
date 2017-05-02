package com.unionpay.etl

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.unionpay.db.jdbc.{JdbcUtil, MysqlConnection}
import com.unionpay.util.ConfigUtil
import net.ceedubs.ficus.readers.ArbitraryTypeReader._

/**
  * Created by ywp on 2016/7/8.
  */
object IndustryInfoJob {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("IndustryInfoJob--行业类、行业子类任务")
      //      .setMaster("local[4]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    etl
    sc.stop()
  }

  def etl(implicit sqlContext: SQLContext) = {

    val industryTb = "TBL_CHMGM_MCHNT_PARA"
    val parentTb = "tbl_content_industry_inf"
    val subTb = "tbl_content_industry_sub_inf"
    val industryDF = JdbcUtil.mysqlJdbcDF(industryTb)
    val config: ParentSubConfig = readConfig.union
    val parentDF = transParentDF(industryDF, config)
    val subDF = transSubDF(industryDF, config)
    save2Mysql(parentDF, parentTb)
    save2Mysql(subDF, subTb)

  }

  def transParentDF(industryDF: DataFrame, config: ParentSubConfig)
                   (implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val parentDF = industryDF.filter($"MCHNT_PARA_LEVEL" === 1)
    val tmpDF = parentDF.reNameColumn(config.parent.transform)
      .appendDefaultVColumn(config.parent.default)
      .addAlwaysColumn
    tmpDF.select($"INDUSTRY_NO".cast(StringType).as("INDUSTRY_NO"),
      $"INDUSTRY_NM", $"INDUSTRY_INTRO",
      $"INDUSTRY_DESC", $"INDUSTRY_IMG",
      $"ROW_CRT_USR", $"ROW_CRT_TS",
      $"REC_UPD_USR", $"REC_UPD_TS")
  }

  def transSubDF(industryDF: DataFrame, config: ParentSubConfig)
                (implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val subDF = industryDF.as('c)
      .join(industryDF.as('p), $"c.MCHNT_PARA_PARENT_ID" === $"p.MCHNT_PARA_ID" and $"p.MCHNT_PARA_LEVEL" === 1, "leftsemi")
      .selectExpr("c.*")
    val tmpDF = subDF.reNameColumn(config.sub.transform)
      .appendDefaultVColumn(config.sub.default)
      .addAlwaysColumn
    tmpDF.select($"INDUSTRY_SUB_NO".cast(StringType).as("INDUSTRY_SUB_NO"),
      $"INDUSTRY_NO".cast(StringType).as("INDUSTRY_NO"),
      $"INDUSTRY_SUB_NM", $"INDUSTRY_SUB_INTRO",
      $"INDUSTRY_SUB_DESC", $"INDUSTRY_SUB_IMG",
      $"ROW_CRT_USR", $"ROW_CRT_TS",
      $"REC_UPD_USR", $"REC_UPD_TS")
  }

  def save2Mysql(df: DataFrame, table: String)(implicit sqlContext: SQLContext) = {
    JdbcUtil.save2Mysql(table)(df)
  }

  def readConfig: IndustryInfoConfig = {
    import net.ceedubs.ficus.Ficus._
    val configName = "industryInfo"
    ConfigUtil.readClassPathConfig[IndustryInfoConfig](configName)
  }
}

case class UnitConfig(transform: Map[String, String], default: Map[String, String])

case class ParentSubConfig(parent: UnitConfig, sub: UnitConfig)

case class IndustryInfoConfig(union: ParentSubConfig)