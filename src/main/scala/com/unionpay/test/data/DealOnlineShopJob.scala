package com.unionpay.test.data

import java.math.{BigDecimal => javaBigDecimal}
import java.sql.Timestamp
import java.util

import com.unionpay.test.data.DealArea._
import com.mongodb.{BasicDBList, BasicDBObject}
import com.unionpay.ShopInfoConfig
import com.unionpay.db.jdbc.{JdbcSaveMode, JdbcUtil}
import com.unionpay.db.mongo.MongoUtil._
import com.unionpay.etl._
import com.unionpay.util.{ConfigUtil, IdGenerator}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Created by ls on 2016/11/4.
  */
object DealOnlineShopJob {

  private lazy val timeRegex ="""\d{1,2}(:|：)\d{2}""".r
  private lazy val dianRegex ="""\d{1,2}点""".r
  private lazy val chRegex ="""[\u4e00-\u9fa5]""".r

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("DealOnlineShopJob === 处理online_04_07数据")
      //      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "32")
    import sqlContext.implicits._

    //    val etlConfig = readConfig

    val onlineShopIds = fetchOnlineShopId.map(_.getAs[String]("MCHNT_CD")).collect()
    val shopDF = JdbcUtil.mysqlJdbcDF("tbl_chmgm_shop_tmp", "sink")
      .filter(!$"MCHNT_NM".contains("验证"))
      .filter(!$"MCHNT_NM".contains("测试"))
      .filter($"MCHNT_CD".isin(onlineShopIds: _*))
      .distinct()

    println("shopDF数目:" + shopDF.count())
    //    val brandTb = "tbl_chmgm_brand_inf"
    //    val brandDF = JdbcUtil.mysqlJdbcDF(brandTb)
    //      .selectExpr("BRAND_ID", "trim(BRAND_NM) BRAND_NM")

    shopDF.selectExpr("MCHNT_CD").write.mode(SaveMode.Overwrite).parquet("/tmp/ls/onlineShop_04")

    //    etl(shopDF, brandDF, etlConfig)
    //    delArea

    sc.stop()
  }

  def etl(shopDF: DataFrame, brandDF: DataFrame, etlConfig: ShopInfoConfig)(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    //店铺编号生成
    val generateShopID = udf(() => IdGenerator.generateShopID)

    sqlContext.udf.register("getShopName", (shopName: String, brandName: String) => getShopName(shopName, brandName))
    sqlContext.udf.register("unionValue", (value: String, new_value: String) => unionValue(value, new_value))
    sqlContext.udf.register("getShopTime", (time: String) => getShopTime(time))

    val tDF = shopDF.as('a)
      .join(brandDF.as('b), $"a.transId" === $"b.BRAND_ID", "left_outer")
      .selectExpr("a.*", "coalesce(b.BRAND_NM,'')  BRAND_NM")
      .reNameColumn(etlConfig.union.reName)
      .selectExpr("SRC_SHOP_NO", "getShopName(unionValue(SHOP_NM,NEW_NAME),BRAND_NM)  SHOP_NM", "MCHNT_SECOND_PARA", "TRANS_UPD_TS",
        "trim(PROV_CD) PROV_CD", "trim(CITY_CD) CITY_CD",
        "case when trim(COUNTY_CD)='' then trim(CITY_CD) else trim(COUNTY_CD) end COUNTY_CD", "SHOP_ADDR", "SHOP_LNT",
        "SHOP_LAT", "getShopTime(unionValue(BUSI_TIME,NEW_BUSS_HOUR)) BUSI_TIME", "PARK_INF", "transId SRC_BRAND_NO",
        "unionValue(SHOP_CONTACT_PHONE,NEW_PHONE) SHOP_CONTACT_PHONE", "SHOP_RATE", "SHOP_AVE_CONSUME", "SHOP_VALID_ST")

    println("tDF数目：" + tDF.count())

    val tmpDF = tDF.mapPartitions(it => {
      val landMark = C("Landmark")
      //      val tradingArea = C("TradingArea")
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
        val landMarkId = if (lng < 1) ""
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
        val mchntSecondPara = row.getAs[Long]("MCHNT_SECOND_PARA")
        val provCd = row.getAs[String]("PROV_CD")
        val cityCd = row.getAs[String]("CITY_CD")
        val countyCd = row.getAs[String]("COUNTY_CD")
        val shopAddr = row.getAs[String]("SHOP_ADDR")
        val busiTime = row.getAs[String]("BUSI_TIME")
        val parkInf = row.getAs[String]("PARK_INF")
        val srcBrandNo = row.getAs[Long]("SRC_BRAND_NO")
        val shopContactPhone = row.getAs[String]("SHOP_CONTACT_PHONE")
        val shopValidSt = row.getAs[String]("SHOP_VALID_ST")
        val shopRate = row.getAs[Int]("SHOP_RATE")
        val shopAveConsume = row.getAs[javaBigDecimal]("SHOP_AVE_CONSUME")
        val transUpdTs = row.getAs[Timestamp]("TRANS_UPD_TS")

        (srcShopNo, shopNm, mchntSecondPara, provCd, cityCd, countyCd, shopAddr,
          lng, lat, busiTime, parkInf, srcBrandNo, shopContactPhone, shopValidSt, shopRate, shopAveConsume, landMarkId, transUpdTs)
      })
    })
      .toDF("SRC_SHOP_NO", "SHOP_NM", "MCHNT_SECOND_PARA", "PROV_CD", "CITY_CD", "COUNTY_CD", "SHOP_ADDR",
        "SHOP_LNT", "SHOP_LAT", "BUSI_TIME", "PARK_INF", "SRC_BRAND_NO", "SHOP_CONTACT_PHONE", "SHOP_VALID_ST", "SHOP_RATE", "SHOP_AVE_CONSUME", "LANDMARK_ID", "TRANS_UPD_TS")
      .appendDefaultVColumn(etlConfig.union.default)
      .addAlwaysColumn

    println("tmpDF数目：" + tmpDF.count())

    val contentBrandDF = fetchBrandDF
    val logicDF = fetchCategoryDf

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
      "industry_no", "industry_sub_no", "ROW_CRT_USR", "ROW_CRT_TS", "REC_UPD_USR", "REC_UPD_TS", "LANDMARK_ID", "TRANS_UPD_TS"
    )

    println("df数目：" + df.count())

    val needDF = df.selectExpr("SRC_SHOP_NO")
    val existedDF = JdbcUtil.mysqlJdbcDF("tbl_content_shop_inf", "sink")
      .filter($"src_shop_tp" === 1).selectExpr("src_shop_no SRC_SHOP_NO")

    val updateDF = needDF.intersect(existedDF)
    val insertDF = needDF.except(existedDF)

    val updateShopDF = df.join(updateDF, df("SRC_SHOP_NO") === updateDF("SRC_SHOP_NO"), "leftsemi")
    val insertShopDF = df.join(insertDF, df("SRC_SHOP_NO") === insertDF("SRC_SHOP_NO"), "leftsemi")
      .withColumn("SHOP_NO", generateShopID())

    // todo 暂时不追加
    save2Mysql(updateShopDF, "tbl_content_shop_inf")
    save2Mysql(insertShopDF, "tbl_content_shop_inf")

    println("updateShopDF: " + updateShopDF.count())
    println("insertShopDF: " + insertShopDF.count())

  }

  def fetchOnlineShopId(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    //    val ts = Seq("04", "07")
    val flatDF = JdbcUtil.mysqlJdbcDF("TBL_CHMGM_CHARA_GRP_DEF_FLAT_sb")
      .filter(s"date_format(REC_CRT_TS,'yyyy-MM-dd') < '2016-09-01'")
      .selectExpr("MCHNT_CD", "CHARA_GRP_CD")
    val couponDF = JdbcUtil.mysqlJdbcDF("TBL_CHMGM_TICKET_COUPON_INF_sb")
      .filter(s"date_format(REC_CRT_TS,'yyyy-MM-dd') < '2016-09-01'")
      .filter("SALE_IN='1'")
      .filter($"BILL_TP" === "04")
      .selectExpr("CHARA_GRP_CD", "SALE_IN", "BILL_TP")
    flatDF.as('a)
      .join(couponDF.as('b), $"a.CHARA_GRP_CD" === $"b.CHARA_GRP_CD", "leftsemi")
      .selectExpr("trim(a.MCHNT_CD)  MCHNT_CD")
  }

  def save2Mysql(df: DataFrame, table: String)(implicit sqlContext: SQLContext) = {
    JdbcUtil.saveToMysql(table, JdbcSaveMode.Upsert)(df)
  }

  def fetchBrandDF(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val brandTB = "tbl_content_brand_inf"
    //银联自有商户有brand_id 互联网只需匹配一个即可
    JdbcUtil.mysqlJdbcDF(brandTB, "sink").selectExpr("trim(brand_no) brand_no", "trim(src_brand_no) src_brand_no")
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

  def readConfig: ShopInfoConfig = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    val configName = "shopInfo"
    ConfigUtil.readClassPathConfig[ShopInfoConfig](configName)
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

  def unionValue(value: String, new_value: String): String = {
    new_value match {
      case null => value
      case "" => value
      case "2" | "2.0" => value
      case _ => new_value
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
