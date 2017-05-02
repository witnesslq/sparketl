package com.unionpay.test.data

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.databricks.spark.csv._
import com.unionpay.ShopInfoConfig
import com.unionpay.db.jdbc.{JdbcSaveMode, JdbcUtil}
import com.unionpay.util.{ConfigUtil, IdGenerator}
import org.apache.spark.sql.functions._
import com.unionpay.etl._

/**
  * Created by ls on 2016/10/21.
  */
object BankShop {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("InsertData to CMS === 插入MongoShop数据")
      //      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")
    sqlContext.udf.register("unionValue", (value: String, new_value: String) => unionValue(value, new_value))


    val areaDF = fetchAreaDF
    val logicDF = fetchCategoryDf
    val brandDF = fetchBrandDF
    val config = readConfig


    val path = "/tmp/ls/bank/03050000.csv"
    val df = sqlContext.csvFile(path, true, ',', '"')
      .selectExpr("shopId  src_shop_no", "shopName", "address", "score", "averagePrice",
        "case when length(description)>50 then substring(description,0,50) else description end description",
        "subCategory", "cast(city as string) city", "trim(tradingArea) tradingArea", "hours busi_time",
        "shop_lnt", "shop_lat", "tags shop_service", "unionValue(brand,new_brand) brand_nm", "pictureList shop_image",
        "unionValue(telephones,new_telephones)  shop_contact_phone", "landId landmark_id", "updateAt"
      )
      .reNameColumn(config.crawl.reName)
      .appendDefaultVColumn(config.crawl.default)

    df.printSchema()
    df.show(5)


    val finalDF = df.as('a)
      .join(logicDF.as('b), $"a.subCategory" === $"b.origin_name" || $"a.subCategory" === $"b.real_name", "left_outer")
      .selectExpr("a.tradingArea", "a.city", "a.src_shop_no", "a.src_shop_tp", "a.shop_nm",
        "a.shop_lnt", "a.shop_lat", "a.busi_time", "a.shop_service", "a.shop_addr", "a.shop_st", "a.shop_valid_st",
        "a.shop_image", "a.shop_contact_phone", "a.brand_nm", "a.shop_tp", "a.shop_valid_dt_st", "a.shop_valid_dt_end",
        "a.shop_desc", "coalesce(b.industry_no,'')  industry_no", "coalesce(b.industry_sub_no,'') industry_sub_no",
        "a.shop_rate", "a.shop_ave_consume*100  shop_ave_consume", "a.landmark_id", "a.trans_upd_ts"
      )

    val cityDF = fetchCityDF

    saveCrawlDB(finalDF, areaDF, brandDF, cityDF)

    sc.stop()

  }


  def saveCrawlDB(mongoDF: DataFrame, areaDF: DataFrame, brandDF: DataFrame, cityDF: DataFrame)(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._

    //店铺编号生成
    val generateShopID = udf(() => IdGenerator.generateShopID)

    val df = mongoDF.as('a)
      .join(broadcast(areaDF).as('b), $"a.tradingArea" === $"b.area_nm" and $"a.city" === $"b.county_cd", "left_outer")
      .join(broadcast(cityDF).as('d), $"a.city" === $"d.ADMIN_DIVISION_CD", "left_outer")
      .join(brandDF.as('c), $"a.brand_nm" === $"c.brand_nm", "left_outer")
      .selectExpr("a.src_shop_no src_shop_no", "a.src_shop_tp", "a.shop_nm", "a.trans_upd_ts",
        "a.shop_lnt", "a.shop_lat", "a.busi_time", "a.shop_service", "a.shop_image", "coalesce(b.area_no,'')  area_no",
        "coalesce(c.brand_no,'')  brand_no", "a.shop_contact_phone", "a.shop_addr", "a.shop_st", "a.shop_valid_st",
        "a.shop_desc", "a.shop_tp", "a.shop_valid_dt_st", "a.shop_rate", "a.shop_ave_consume", "coalesce(d.PROV_DIVISION_CD,'') as prov_cd",
        "coalesce(d.CITY_DIVISION_CD,'') as city_cd", "coalesce(d.ADMIN_DIVISION_CD,'') as county_cd", "a.industry_no", "a.industry_sub_no",
        "a.landmark_id"
      )
      .withColumn("shop_no", generateShopID())
      .addAlwaysColumn

    save2Mysql(df, "tbl_content_shop_inf")
  }

  def save2Mysql(df: DataFrame, table: String)(implicit sqlContext: SQLContext) = {
    JdbcUtil.saveToMysql(table, JdbcSaveMode.Upsert)(df)
  }

  def fetchAreaDF(implicit sqlContext: SQLContext): DataFrame = {
    val areaTB = "tbl_content_busi_area"
    JdbcUtil.mysqlJdbcDF(areaTB, "sink")
      .selectExpr("trim(area_no) area_no", "trim(area_nm) area_nm", "trim(county_cd) county_cd", "trim(city_cd) city_cd")
  }

  def fetchBrandDF(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val brandTB = "tbl_content_brand_inf"
    //银联自有商户有brand_id 互联网只需匹配一个即可
    JdbcUtil.mysqlJdbcDF(brandTB, "sink").selectExpr("trim(brand_no) brand_no", "trim(brand_nm) brand_nm")
      .groupBy($"brand_nm").agg(first($"brand_no").as("brand_no"))
  }


  def fetchSubIndustryDF(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val subTb = "tbl_content_industry_sub_inf"
    JdbcUtil.mysqlJdbcDF(subTb, "sink").selectExpr("industry_no", "industry_sub_no", "industry_sub_nm")
  }

  def fetchCategoryDf(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val subDF = fetchSubIndustryDF
    val industryLogicDF = fetchIndustryLogic
    industryLogicDF.as('a)
      .join(subDF.as('b), trim($"a.real_name") === trim($"b.industry_sub_nm"))
      .selectExpr("trim(a.origin_name)  origin_name", "trim(a.real_name) real_name", "trim(b.industry_sub_nm)  industry_sub_nm",
        "trim(b.industry_no) industry_no", "trim(b.industry_sub_no)  industry_sub_no")
  }

  def fetchIndustryLogic(implicit sqlContext: SQLContext): DataFrame = {
    JdbcUtil.mysqlJdbcDF("tbl_industry_etl_logic", "sink").selectExpr("type", "type_name", "origin_name", "real_name")
  }

  def fetchCityDF(implicit sqlContext: SQLContext): DataFrame = {
    val cityTB = "TBL_CHMGM_BUSS_ADMIN_DIVISION_CD"
    JdbcUtil.mysqlJdbcDF(cityTB)
  }

  def unionValue(item: String, new_item: String) = {
    new_item match {
      case null => item
      case "" => item
      case _ => new_item
    }
  }

  def readConfig: ShopInfoConfig = {
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import net.ceedubs.ficus.Ficus._
    val configName = "shopInfo"
    ConfigUtil.readClassPathConfig[ShopInfoConfig](configName)
  }

}