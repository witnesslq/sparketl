package com.unionpay.test.ls

import java.math.{RoundingMode, BigDecimal => javaBigDecimal}
import java.sql.Timestamp
import java.util

import com.mongodb.{BasicDBList, BasicDBObject}
import com.unionpay.db.jdbc.{JdbcSaveMode, JdbcUtil}
import com.unionpay.db.mongo.MongoUtil._
import com.unionpay.util.{ConfigUtil, IdGenerator}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import org.apache.spark.sql.functions._
import com.unionpay._
import org.apache.spark.sql.types.StringType
import com.unionpay.etl._

import scala.collection.JavaConversions._

/**
  * Created by ls on 2016/11/29.
  */
object InsertNewShopData {


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
      //      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2000m")
      .set("spark.yarn.driver.memoryOverhead", "2048")
      .set("spark.yarn.executor.memoryOverhead", "2000")
      .set("spark.network.timeout", "300s")
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
    etlUnion(config, areaDF, logicDF)
  }


  def etlUnion(etlConfig: ShopInfoConfig, areaDF: DataFrame, logicDF: DataFrame)(implicit sqlContext: SQLContext) = {

    import sqlContext.implicits._

    //店铺编号生成
    val generateShopID = udf(() => IdGenerator.generateShopID)
    //获取营业时间
    sqlContext.udf.register("getShopTime", (time: String) => getShopTime(time))
    //获取停车状态
    sqlContext.udf.register("getServices", (park: String) => getServices(park))
    //获取商店名信息
    sqlContext.udf.register("getShopName", (shopName: String, brandName: String) => getShopName(shopName, brandName))

    val unionShopTB = "TBL_CHMGM_PREFERENTIAL_MCHNT_INF"
    //商户名带有 验证 测试  直接去掉
    val shopDF = JdbcUtil.mysqlJdbcDF(unionShopTB)
      .reNameColumn(etlConfig.union.reName)
      .filter(!$"SHOP_NM".contains("验证"))
      .filter(!$"SHOP_NM".contains("测试"))
    //品牌信息
    val brandTb = "tbl_content_brand_inf"
    val brandDF = JdbcUtil.mysqlJdbcDF(brandTb)
      .filter("src_brand_tp='1'")
      .selectExpr("BRAND_NO BRAND_ID", "trim(BRAND_NM) BRAND_NM", "SRC_BRAND_NO")

    val tDF = shopDF.as('a)
      .join(brandDF.as('b), $"a.BRAND_NO".cast(StringType) === $"b.SRC_BRAND_NO", "left_outer")
      .selectExpr("a.*", "coalesce(b.BRAND_NM,'')  BRAND_NM", "coalesce(b.BRAND_ID,'') BRAND_ID")

    val tmpDF = tDF.filter("mchnt_tp='0'")
      .selectExpr("SRC_SHOP_NO", "getShopName(SHOP_NM,BRAND_NM)  SHOP_NM", "MCHNT_SECOND_PARA", "TRANS_UPD_TS",
        "trim(PROV_CD) PROV_CD", "trim(CITY_CD) CITY_CD",
        "case when trim(COUNTY_CD)='' then trim(CITY_CD) else trim(COUNTY_CD) end COUNTY_CD", "SHOP_ADDR", "SHOP_LNT",
        "SHOP_LAT", "getShopTime(BUSI_TIME) BUSI_TIME", "PARK_INF", "BRAND_ID BRAND_NO", "SHOP_CONTACT_PHONE",
        "MCHNT_ST", "SHOP_RATE", "SHOP_AVE_CONSUME")
      .mapPartitions(it => {
        val col = C("Landmark")
        it.map(row => {
          val myLocation = new BasicDBList()
          val lng = row.getAs[javaBigDecimal]("SHOP_LNT").doubleValue()
          val lat = row.getAs[javaBigDecimal]("SHOP_LAT").doubleValue()
          myLocation.put(0, lng)
          myLocation.put(1, lat)
          val geoNear = new BasicDBObject("$geoNear",
            new BasicDBObject("near", myLocation)
              .append("limit", 1)
              .append("distanceField", "centerCoordinates")
              .append("maxDistance", 1000 / 6378137))
          val landMarkId = col.aggregate(util.Arrays.asList(geoNear))
            .useCursor(true)
            .iterator()
            .map(doc => {
              doc.getObjectId("_id").toString
            }).toStream.headOption getOrElse ("")
          val srcShopNo = row.getAs[String]("SRC_SHOP_NO")
          val shopNm = row.getAs[String]("SHOP_NM")
          val mchntSecondPara = row.getAs[Long]("MCHNT_SECOND_PARA")
          val provCd = row.getAs[String]("PROV_CD")
          val cityCd = row.getAs[String]("CITY_CD")
          val countyCd = row.getAs[String]("COUNTY_CD")
          val shopAddr = row.getAs[String]("SHOP_ADDR")
          val busiTime = row.getAs[String]("BUSI_TIME")
          val parkInf = row.getAs[String]("PARK_INF")
          val brandNo = row.getAs[String]("BRAND_NO")
          val shopContactPhone = row.getAs[String]("SHOP_CONTACT_PHONE")
          val mchntSt = row.getAs[String]("MCHNT_ST")
          val shopRate = row.getAs[Int]("SHOP_RATE")
          val shopAveConsume = row.getAs[javaBigDecimal]("SHOP_AVE_CONSUME")
          val transUpdTs = row.getAs[Timestamp]("TRANS_UPD_TS")

          (srcShopNo, shopNm, mchntSecondPara, provCd, cityCd, countyCd, shopAddr,
            lng, lat, busiTime, parkInf, brandNo, shopContactPhone, mchntSt, shopRate, shopAveConsume, landMarkId, transUpdTs)
        })
      })
      .toDF("SRC_SHOP_NO", "SHOP_NM", "MCHNT_SECOND_PARA", "PROV_CD", "CITY_CD", "COUNTY_CD", "SHOP_ADDR",
        "SHOP_LNT", "SHOP_LAT", "BUSI_TIME", "PARK_INF", "BRAND_NO", "SHOP_CONTACT_PHONE", "MCHNT_ST", "SHOP_RATE", "SHOP_AVE_CONSUME", "LANDMARK_ID", "TRANS_UPD_TS")
      .appendDefaultVColumn(etlConfig.union.default)
      .addAlwaysColumn

    val originIndustryDF = JdbcUtil.mysqlJdbcDF("TBL_CHMGM_MCHNT_PARA")
    val tmpSubDF = originIndustryDF.as('c)
      .join(originIndustryDF.as('p), $"c.MCHNT_PARA_PARENT_ID" === $"p.MCHNT_PARA_ID" and $"p.MCHNT_PARA_LEVEL" === 1, "leftsemi")
      .selectExpr("c.*")

    val subDF = tmpSubDF.as('t)
      .join(logicDF.as('l), trim($"t.MCHNT_PARA_CN_NM") === $"l.origin_name")
      .selectExpr("t.MCHNT_PARA_ID", "l.industry_no", "l.industry_sub_no")

    val finalDF = tmpDF.as('a)
      .join(broadcast(subDF).as('c), $"a.MCHNT_SECOND_PARA" === $"c.MCHNT_PARA_ID", "left_outer")
      .selectExpr("a.*", "coalesce(c.industry_no,'') industry_no", "coalesce(c.industry_sub_no,'') industry_sub_no")

    val df = finalDF.selectExpr("SRC_SHOP_NO", "SRC_SHOP_TP", "SHOP_NM",
      "PROV_CD", "CITY_CD", "COUNTY_CD", "SHOP_ADDR", "case when SHOP_LNT=0 then '' else cast(SHOP_LNT as string) end SHOP_LNT",
      "case when SHOP_LAT=0 then '' else cast(SHOP_LAT as string) end SHOP_LAT",
      "case when length(BUSI_TIME)>60 then substring(BUSI_TIME,0,60) else BUSI_TIME end BUSI_TIME",
      "getServices(PARK_INF) AS SHOP_SERVICE", "SHOP_IMAGE",
      "area_no", "case when BRAND_NO='0' then ''  else BRAND_NO  end BRAND_NO",
      "substring(trim(SHOP_CONTACT_PHONE),0,20) SHOP_CONTACT_PHONE", "SHOP_ST",
      "case when MCHNT_ST='2' then '1' else '0' end  SHOP_VALID_ST", "SHOP_TP", "SHOP_VALID_DT_ST", "SHOP_VALID_DT_END",
      "case when SHOP_RATE=0 then '' else cast(SHOP_RATE as string) end SHOP_RATE", "yuan2Fen(SHOP_AVE_CONSUME) SHOP_AVE_CONSUME",
      "industry_no", "industry_sub_no", "ROW_CRT_USR", "ROW_CRT_TS", "REC_UPD_USR", "REC_UPD_TS", "LANDMARK_ID", "TRANS_UPD_TS"
    )

    val needDF = df.selectExpr("SRC_SHOP_NO")
    val existedDF = JdbcUtil.mysqlJdbcDF("tbl_content_shop_inf", "sink")
      .filter($"src_shop_tp" === lit("1")).selectExpr("src_shop_no SRC_SHOP_NO")

    val updateDF = needDF.intersect(existedDF)
    val insertDF = needDF.except(existedDF)

    val updateShopDF = df.join(updateDF, df("SRC_SHOP_NO") === updateDF("SRC_SHOP_NO"), "leftsemi")
    val insertShopDF = df.join(insertDF, df("SRC_SHOP_NO") === insertDF("SRC_SHOP_NO"), "leftsemi")
      .withColumn("SHOP_NO", generateShopID())

    save2Mysql(updateShopDF, "tbl_content_shop_inf")
    save2Mysql(insertShopDF, "tbl_content_shop_inf")

    val onlineShopDF = shopDF.filter("mchnt_tp='1'")
      .selectExpr("trim(MCHNT_CD) src_shop_no", "trim(MCHNT_NM) shop_nm", "TRANS_UPD_TS",
        "case when length(trim(MCHNT_PHONE))>15 then '' else trim(split(cast(MCHNT_PHONE as string),',')[0]) end  shop_contact_phone",
        "case when MCHNT_ST='2' then '1' else '0' end shop_valid_st", "COMMENT_VALUE shop_rate"
      ).addAlwaysColumn

    val onlineNeedDF = onlineShopDF.selectExpr("src_shop_no")
    val onlineExistedDF = JdbcUtil.mysqlJdbcDF("tbl_content_online_shop", "sink").selectExpr("src_shop_no")

    val onlineUpdateDF = onlineNeedDF.intersect(onlineExistedDF)
    val onlineInsertDF = onlineNeedDF.except(onlineExistedDF)

    val onlineUpdateShopDF = onlineShopDF.join(onlineUpdateDF, onlineShopDF("src_shop_no") === onlineUpdateDF("src_shop_no"), "leftsemi")
    val onlineInsertShopDF = onlineShopDF.join(onlineInsertDF, onlineShopDF("src_shop_no") === onlineInsertDF("src_shop_no"), "leftsemi")
      .withColumn("SHOP_NO", generateShopID())

    save2Mysql(onlineUpdateShopDF, "tbl_content_online_shop")
    save2Mysql(onlineInsertShopDF, "tbl_content_online_shop")

    val shopAreaDF = df.selectExpr("SRC_SHOP_NO", "SHOP_LNT", "SHOP_LAT")
      .mapPartitions(it => {
        val tradingArea = C("TradingArea")
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
              .append("maxDistance", 2000 / 6378137))
          val areaId = if (lg < 1) ""
          else {
            tradingArea.aggregate(util.Arrays.asList(geoNear))
              .useCursor(true)
              .iterator()
              .map(doc => {
                doc.getObjectId("_id").toString
              }).toStream.headOption getOrElse ("")
          }

          val shopId = row.getAs[String]("SRC_SHOP_NO")
          (shopId, areaId)
        })
      }).toDF("SRC_SHOP_NO", "areaId")

    val updateAreaDF = shopAreaDF.as('a)
      .join(areaDF.as('b), $"a.areaId" === $"b.src_area_no", "left_outer")
      .selectExpr("a.SRC_SHOP_NO", "coalesce(b.area_no,'') area_no")

    JdbcUtil.saveToMysql("tbl_content_shop_inf", JdbcSaveMode.Upsert)(updateAreaDF)
  }


  def yuan2cent(y: javaBigDecimal) = {
    val rounded = y.setScale(2, RoundingMode.CEILING)
    val bigDecimalInCents = rounded.multiply(x100)
    bigDecimalInCents.intValueExact()
  }

  def save2Mysql(df: DataFrame, table: String)(implicit sqlContext: SQLContext) = {
    JdbcUtil.saveToMysql(table, JdbcSaveMode.Upsert)(df)
  }


  def getServices(park: String) = {
    Option(park) match {
      case None => "00000"
      case Some(s) => {
        s.replaceAll(" ", "") match {
          case " " | "--" => "00000"
          case x: String if x.contains("无") || x.contains("询") => "00000"
          case _ => "00010"
        }
      }
    }
  }


  def getShopName(shopName: String, brandName: String)(implicit sqlContext: SQLContext) = {
    val spName = shopName.replaceAll(" ", "")
    val spBrand = brandName.replaceAll(" ", "")
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

  def readConfig: ShopInfoConfig = {
    import net.ceedubs.ficus.Ficus._
    val configName = "shopInfo"
    ConfigUtil.readClassPathConfig[ShopInfoConfig](configName)
  }


  def fetchOnlineShopId(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val flatDF = JdbcUtil.mysqlJdbcDF("TBL_CHMGM_CHARA_GRP_DEF_FLAT").selectExpr("MCHNT_CD", "CHARA_GRP_CD")
    val couponDF = JdbcUtil.mysqlJdbcDF("TBL_CHMGM_TICKET_COUPON_INF")
      .filter("SALE_IN='1'")
      .filter($"BILL_TP" === "08")
      .selectExpr("CHARA_GRP_CD", "SALE_IN", "BILL_TP")
    flatDF.as('a)
      .join(couponDF.as('b), $"a.CHARA_GRP_CD" === $"b.CHARA_GRP_CD", "leftsemi")
      .selectExpr("trim(a.MCHNT_CD)  MCHNT_CD")
  }

  def fetchAreaDF(implicit sqlContext: SQLContext): DataFrame = {
    val areaTB = "tbl_content_busi_area"
    JdbcUtil.mysqlJdbcDF(areaTB, "sink")
      .selectExpr("trim(area_no) area_no", "trim(county_cd) county_cd", "trim(city_cd) city_cd")
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
