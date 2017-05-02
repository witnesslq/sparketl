package com.unionpay.test.online

import java.math.{BigDecimal => javaBigDecimal}
import java.util

import com.mongodb.{BasicDBList, BasicDBObject}
import com.unionpay._
import com.unionpay.db.jdbc.{JdbcSaveMode, JdbcUtil}
import com.unionpay.db.mongo.MongoUtil._
import com.unionpay.etl._
import com.unionpay.test.data.DealArea._
import com.unionpay.util.{ConfigUtil, IdGenerator}
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Created by ls on 2016/11/29.
  */
object InsertContentShop {

  private lazy val timeRegex ="""\d{1,2}(:|：)\d{2}""".r
  private lazy val dianRegex ="""\d{1,2}点""".r
  private lazy val chRegex ="""[\u4e00-\u9fa5]""".r

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("InsertData to CMS === 插入数据")
      //      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "32")

    delShop
    delArea

    sc.stop()
  }

  def delShop(implicit sqlContext: SQLContext) = {

    import sqlContext.implicits._

    val etlConfig = readConfig
    val logicDF = fetchCategoryDf

    //店铺编号生成
    val generateShopID = udf(() => IdGenerator.generateShopID)
    //互联网
    val onlineShopIdDF = fetchOnlineShopId
    val onlineShopIds = onlineShopIdDF.map(_.getAs[String]("MCHNT_CD")).collect()

    val shopDF = sqlContext.read.parquet("/tmp/ls/result_db2mysql_1")
      .filter(!$"MCHNT_NM".contains("验证"))
      .filter(!$"MCHNT_NM".contains("测试"))

    println("shopDF:" + shopDF.count())


    val tTmpDF = shopDF
      .filter(!$"MCHNT_CD".isin(onlineShopIds: _*))
      .reNameColumn(etlConfig.union.reName)
      .selectExpr("SRC_SHOP_NO", "SHOP_NM", "MCHNT_SECOND_PARA", /*"TRANS_UPD_TS",*/
        "trim(PROV_CD) PROV_CD", "trim(CITY_CD) CITY_CD",
        "case when trim(COUNTY_CD)='' then trim(CITY_CD) else trim(COUNTY_CD) end COUNTY_CD", "SHOP_ADDR", "SHOP_LNT",
        "SHOP_LAT", "BUSI_TIME", "PARK_INF", "BRAND_NO", "SHOP_CONTACT_PHONE", "SHOP_RATE", "cast(SHOP_AVE_CONSUME as double) SHOP_AVE_CONSUME", "SHOP_VALID_ST")

    println("tTmpDF:" + tTmpDF.count())

    val tmpDF = tTmpDF.mapPartitions(it => {
      val landMark = C("Landmark")
      //      val tradingArea = C("TradingArea")
      it.map(row => {
        val myLocation = new BasicDBList()
        val lng = row.getAs[String]("SHOP_LNT")
        val lg = if (lng.trim.isEmpty) 0 else lng.trim.toDouble
        val lat = row.getAs[String]("SHOP_LAT")
        val lt = if (lat.trim.isEmpty) 0 else lat.trim.toDouble
        myLocation.put(0, lg)
        myLocation.put(1, lt)
        val geoNear = new BasicDBObject("$geoNear",
          new BasicDBObject("near", myLocation)
            .append("limit", 1)
            .append("distanceField", "centerCoordinates")
            .append("maxDistance", 1000 / 6378137))
        val landMarkId = if (lg < 1) ""
        else {
          landMark.aggregate(util.Arrays.asList(geoNear))
            .useCursor(true)
            .iterator()
            .map(doc => {
              doc.getObjectId("_id").toString
            }).toStream.headOption getOrElse ("")
        }
        val srcShopNo = row.getAs[String]("SRC_SHOP_NO")
        val shopNm = row.getAs[String]("SHOP_NM")
        val mchntSecondPara = row.getAs[String]("MCHNT_SECOND_PARA")
        val provCd = row.getAs[String]("PROV_CD")
        val cityCd = row.getAs[String]("CITY_CD")
        val countyCd = row.getAs[String]("COUNTY_CD")
        val shopAddr = row.getAs[String]("SHOP_ADDR")
        val busiTime = row.getAs[String]("BUSI_TIME")
        val parkInf = row.getAs[String]("PARK_INF")
        val srcBrandNo = row.getAs[String]("BRAND_NO")
        val shopContactPhone = row.getAs[String]("SHOP_CONTACT_PHONE")
        val shopValidSt = row.getAs[String]("SHOP_VALID_ST")
        val shopRate = row.getAs[String]("SHOP_RATE")
        val shopAveConsume = row.getAs[Double]("SHOP_AVE_CONSUME")
        /*val transUpdTs = row.getAs[String]("TRANS_UPD_TS")*/

        (srcShopNo, shopNm, mchntSecondPara, provCd, cityCd, countyCd, shopAddr,
          lng, lat, busiTime, parkInf, srcBrandNo, shopContactPhone, shopValidSt, shopRate, shopAveConsume, landMarkId /*, transUpdTs*/ )
      })
    })
      .toDF("SRC_SHOP_NO", "SHOP_NM", "MCHNT_SECOND_PARA", "PROV_CD", "CITY_CD", "COUNTY_CD", "SHOP_ADDR",
        "SHOP_LNT", "SHOP_LAT", "BUSI_TIME", "PARK_INF", "SRC_BRAND_NO", "SHOP_CONTACT_PHONE", "SHOP_VALID_ST", "SHOP_RATE", "SHOP_AVE_CONSUME", "LANDMARK_ID" /*, "TRANS_UPD_TS"*/)
      .appendDefaultVColumn(etlConfig.union.default)
      .addAlwaysColumn

    val contentBrandDF = fetchBrandDF

    val originIndustryDF = JdbcUtil.mysqlJdbcDF("TBL_CHMGM_MCHNT_PARA")
    val tmpSubDF = originIndustryDF.as('c)
      .join(originIndustryDF.as('p), $"c.MCHNT_PARA_PARENT_ID" === $"p.MCHNT_PARA_ID" and $"p.MCHNT_PARA_LEVEL" === 1, "leftsemi")
      .selectExpr("c.*")

    val subDF = tmpSubDF.as('t)
      .join(logicDF.as('l), trim($"t.MCHNT_PARA_CN_NM") === $"l.origin_name")
      .selectExpr("t.MCHNT_PARA_ID", "l.industry_no", "l.industry_sub_no")

    val finalDF = tmpDF.as('a)
      .join(broadcast(subDF).as('c), $"a.MCHNT_SECOND_PARA" === $"c.MCHNT_PARA_ID", "left_outer")
      .join(broadcast(contentBrandDF).as('d), $"a.SRC_BRAND_NO" === $"d.src_brand_no", "left_outer")
      .selectExpr("a.*", "coalesce(d.brand_no,'')  BRAND_NO", "coalesce(c.industry_no,'') industry_no", "coalesce(c.industry_sub_no,'') industry_sub_no")
      .drop("SRC_BRAND_NO")

    val df = finalDF.selectExpr("SRC_SHOP_NO", "SRC_SHOP_TP", "SHOP_NM",
      "PROV_CD", "CITY_CD", "COUNTY_CD", "SHOP_ADDR", "case when SHOP_LNT=0 then '' else cast(SHOP_LNT as string) end SHOP_LNT",
      "case when SHOP_LAT=0 then '' else cast(SHOP_LAT as string) end SHOP_LAT",
      "case when length(BUSI_TIME)>60 then substring(BUSI_TIME,0,60) else BUSI_TIME end BUSI_TIME",
      "PARK_INF SHOP_SERVICE", "SHOP_IMAGE", "case when BRAND_NO=0 then ''  else cast(BRAND_NO as string) end BRAND_NO",
      "substring(trim(SHOP_CONTACT_PHONE),0,20) SHOP_CONTACT_PHONE", "SHOP_ST",
      "SHOP_VALID_ST", "SHOP_TP", "SHOP_VALID_DT_ST", "SHOP_VALID_DT_END",
      "case when SHOP_RATE=0 then '' else cast(SHOP_RATE as string) end SHOP_RATE", "SHOP_AVE_CONSUME",
      "industry_no", "industry_sub_no", "ROW_CRT_USR", "ROW_CRT_TS", "REC_UPD_USR", "REC_UPD_TS", "LANDMARK_ID" /*, "TRANS_UPD_TS"*/
    ).withColumn("SHOP_NO", generateShopID())
    //      .drop("trans_upd_ts")

    println("shopDF Count:" + df.count())

    save2Mysql(df, "tbl_content_shop_inf_20161129")

    val onlineShopDF = shopDF
      .join(onlineShopIdDF, shopDF("MCHNT_CD") === onlineShopIdDF("MCHNT_CD"), "leftsemi")
      .selectExpr("trim(MCHNT_CD) src_shop_no", "trim(MCHNT_NM) shop_nm",
        "substring(trim(MCHNT_PHONE),0,15) shop_contact_phone",
        "'1' shop_valid_st", "COMMENT_VALUE shop_rate" /*, "REC_UPD_TS TRANS_UPD_TS"*/
      ).addAlwaysColumn
      .withColumn("SHOP_NO", generateShopID())

    println("onlineShopDF Count:" + onlineShopDF.count())

    save2Mysql(onlineShopDF, "tbl_content_online_shop_20161129")
  }

  def readConfig: ShopInfoConfig = {
    import net.ceedubs.ficus.Ficus._
    val configName = "shopInfo"
    ConfigUtil.readClassPathConfig[ShopInfoConfig](configName)
  }

  def save2Mysql(df: DataFrame, table: String)(implicit sqlContext: SQLContext) = {
    JdbcUtil.saveToMysql(table, JdbcSaveMode.Upsert)(df)
  }

  def fetchOnlineShopId(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    //    val ts = Seq("04", "07", "08")
    val flatDF = sqlContext.read.parquet("/tmp/ls/TBL_CHMGM_CHARA_GRP_DEF_FLAT")
      .selectExpr("MCHNT_CD", "CHARA_GRP_CD")

    val couponDF = JdbcUtil.mysqlJdbcDF("TBL_CHMGM_TICKET_COUPON_INF")
      .filter("SALE_IN='1'")
      .filter($"BILL_TP" === "08")
      .selectExpr("CHARA_GRP_CD", "SALE_IN", "BILL_TP")

    val couponDF10251111 = JdbcUtil.mysqlJdbcDF("TBL_CHMGM_TICKET_COUPON_INF_10251111")
      .filter("SALE_IN='1'")
      .filter($"BILL_TP" === "08")
      .selectExpr("CHARA_GRP_CD", "SALE_IN", "BILL_TP")

    val cDF = couponDF.unionAll(couponDF10251111)

    flatDF.as('a)
      .join(cDF.as('b), $"a.CHARA_GRP_CD" === $"b.CHARA_GRP_CD", "leftsemi")
      .selectExpr("trim(a.MCHNT_CD)  MCHNT_CD")
  }

  def fetchBrandDF(implicit sqlContext: SQLContext): DataFrame = {
    sqlContext.read.parquet("/tmp/ls/tbl_content_brand_inf29")
      .selectExpr("trim(brand_no) brand_no", "trim(src_brand_no) src_brand_no")
  }

  def fetchSubIndustryDF(implicit sqlContext: SQLContext): DataFrame = {
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
