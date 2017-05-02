package com.unionpay.test.online

import java.math.RoundingMode
import java.math.{BigDecimal => javaBigDecimal}

import com.unionpay.db.jdbc.{JdbcSaveMode, JdbcUtil}
import com.unionpay.util.{ConfigUtil, IdGenerator}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import com.unionpay._
import com.unionpay.etl._

/**
  * Created by ls on 2016/12/1.
  */
object BankShopInfoJob {


  val serviceMap = Map("免费停车" -> "停车", "无线上网" -> "WIFI支持", "可以刷卡" -> "刷卡")
  val default = "云闪付;免密免签;WIFI支持;停车;刷卡;"
  val ds = default.split(";")
  val x100 = new javaBigDecimal(100)

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("shopInfoJob--店铺信息任务")
      //            .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2000m")
      .set("spark.yarn.driver.memoryOverhead", "2048")
      .set("spark.yarn.executor.memoryOverhead", "2000")
      .set("spark.network.timeout", "300s")
      //todo 云主机 经常网络超时
      .set("spark.executor.heartbeatInterval", "30s")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", spark_shuffle_partitions)
    etl(args)
    sc.stop()
  }

  def etl(args: Array[String])(implicit sqlContext: SQLContext) = {

    val config = readConfig

    //元转分
    sqlContext.udf.register("yuan2Fen", (yuan: javaBigDecimal) => yuan2cent(yuan))
    val areaDF = fetchAreaDF
    val logicDF = fetchCategoryDf
    val brandDF = fetchBrandDF
    etlCrawl(config, areaDF, brandDF, logicDF)
  }


  def etlCrawl(etlConfig: ShopInfoConfig, areaDF: DataFrame, brandDF: DataFrame, logicDF: DataFrame)(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    //店铺提供服务
    sqlContext.udf.register("hasServices", (tags: Seq[String]) => shopService(tags))
    //todo 数据有可能重复 见MongoUtil
    val mongoDF = sqlContext.read.parquet("/tmp/ls/mongo_bank_shop")
      .filter($"_id.oid" === "57a05bed951f59b1cfc3d901")
      .distinct()

    println("parquet count:" + mongoDF.count())

    //去掉处理过的数据
    val tmpMongoDF = mongoDF
      //todo 匹配包含英文字母、中文的其他的过滤  杭州安规则来
      //      .filter($"shopName".rlike("""[\u4e00-\u9fa5a-zA-Z]"""))
      .selectExpr("_id.oid  src_shop_no", "shopName", "address", "case when score=0 then '' else cast(score as string) end score",
      "averagePrice", " case when length(trim(description))>1024 then substring(trim(description),0,1024) else trim(description) end description",
      "case when size(subCategory)=0 then '' else trim(subCategory[0]) end subCategory",
      "cast(city as string) city", "trim(tradingArea) tradingArea",
      "case when length(trim(regexp_replace(hours,'：',':')))>60 then substring(trim(regexp_replace(hours,'：',':')),0,60) else trim(regexp_replace(hours,'：',':')) end busi_time",
      "case when size(coordinates)=0 then '' else substring(cast(coordinates[0] as string),0,20) end shop_lnt",
      "case when size(coordinates)<2 then ''   else substring(cast(coordinates[1] as string),0,20) end shop_lat",
      "case when size(tags)=0 then '00000' else hasServices(tags) end shop_service", "trim(brand) brand_nm",
      //      "case when instr(pictureList[0],';')>0 then split(pictureList[0],';')[0] when size(pictureList)=0 then ''  else pictureList[0] end  shop_image",
      "case when instr(telephones[0],',')>0 then trim(split(telephones[0],',')[0]) when instr(telephones[0],' ')>0  then trim(split(telephones[0],' ')[0])  when size(telephones)=0 then '' else  substring(trim(telephones[0]),0,20) end  shop_contact_phone",
      "coalesce(landArr,'') landmark_id", "updateAt"
    ).withColumn("shop_image", lit(""))


    val tmpDF = tmpMongoDF
      .reNameColumn(etlConfig.crawl.reName)
      .appendDefaultVColumn(etlConfig.crawl.default)
      .distinct()
    //todo 先用hours字段做展示使用，后续处理需要dealOpeningHours
    //      .dealOpeningHours

    val ids1 = tmpDF.selectExpr("src_shop_no").map(_.getAs[String]("src_shop_no")).collect()
    println("ids1 count:" + ids1.size)

    val finalDF = tmpDF.as('a)
      .join(logicDF.as('b), $"a.subCategory" === $"b.origin_name" || $"a.subCategory" === $"b.real_name", "left_outer")
      .selectExpr("a.tradingArea", "a.city", "a.src_shop_no", "a.src_shop_tp", "a.shop_nm",
        "a.shop_lnt", "a.shop_lat", "a.busi_time", "a.shop_service", "a.shop_addr", "a.shop_st", "a.shop_valid_st",
        "a.shop_image", "a.shop_contact_phone", "a.brand_nm", "a.shop_tp", "a.shop_valid_dt_st", "a.shop_valid_dt_end",
        "a.shop_desc", "coalesce(b.industry_no,'')  industry_no", "coalesce(b.industry_sub_no,'') industry_sub_no",
        "a.shop_rate", "a.shop_ave_consume*100  shop_ave_consume", "a.landmark_id", "a.trans_upd_ts"
      ).distinct()

    val ids2 = finalDF.selectExpr("src_shop_no").map(_.getAs[String]("src_shop_no")).collect()
    println("join后ids2 count:" + ids2.size)

    val cityDF = fetchCityDF

    saveCrawlDB(finalDF, areaDF, brandDF, cityDF)

  }

  def saveCrawlDB(mongoDF: DataFrame, areaDF: DataFrame, brandDF: DataFrame, cityDF: DataFrame)(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._

    //店铺编号生成
    val generateShopID = udf(() => IdGenerator.generateShopID)

    //todo 2000万数据在测试环境内存不够 广播join的相对较小的表、设大启动的executor内存
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
      ).withColumn("shop_no", generateShopID())
      .addAlwaysColumn
      .distinct()

    val id3 = df.selectExpr("src_shop_no").map(_.getAs[String]("src_shop_no")).collect()
    println("插入数据库之前:" + id3.size)
    save2Mysql(df, "tbl_content_shop_inf_20161201")
  }


  def save2Mysql(df: DataFrame, table: String)(implicit sqlContext: SQLContext) = {
    //    JdbcUtil.save2Mysql(table)(df)
    JdbcUtil.saveToMysql(table, JdbcSaveMode.Upsert)(df)
  }

  def fetchCityDF(implicit sqlContext: SQLContext): DataFrame = {
    val cityTB = "TBL_CHMGM_BUSS_ADMIN_DIVISION_CD"
    JdbcUtil.mysqlJdbcDF(cityTB)
  }

  def shopService(tags: Seq[String]) = {
    val hasServices = serviceMap.filterKeys(tags.contains).map(_._2).toSeq
    val result = ds.map(s => {
      if (hasServices.contains(s)) 1
      else 0
    })
    result.mkString("")
  }


  def yuan2cent(y: javaBigDecimal) = {
    val rounded = y.setScale(2, RoundingMode.CEILING)
    val bigDecimalInCents = rounded.multiply(x100)
    bigDecimalInCents.intValueExact()
  }

  def readConfig: ShopInfoConfig = {
    import net.ceedubs.ficus.Ficus._
    val configName = "shopInfo"
    ConfigUtil.readClassPathConfig[ShopInfoConfig](configName)
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
}
