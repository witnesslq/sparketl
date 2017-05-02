package com.unionpay.etl

import java.math.RoundingMode
import java.math.{BigDecimal => javaBigDecimal}

import com.unionpay.db.jdbc.JdbcUtil
import com.unionpay.util.{ConfigUtil, IdGenerator}
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.unionpay.db.mongo.MongoUtil._
import com.unionpay.etl._

/**
  * 依赖店铺去重训练的两个模型
  *
  * Created by ywp on 2016/7/1.
  */
object ShopInfoJob_test {

  val serviceMap = Map("免费停车" -> "停车", "无线上网" -> "WIFI支持", "可以刷卡" -> "刷卡")
  val default = "云闪付;免密免签;WIFI支持;停车;刷卡;"
  val ds = default.split(";")
  val x100 = new javaBigDecimal(100)

  private lazy val timeRegex ="""\d{1,2}(:|：)\d{2}""".r
  private lazy val dianRegex ="""\d{1,2}点""".r
  private lazy val chRegex ="""[\u4e00-\u9fa5]""".r

  def main(args: Array[String]) {

    if (args.size != 3) {
      System.err.println("useAge: crawl true or false unionpay true or false jobDateTime")
      System.exit(1)
    }

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
    etl(args)
    sc.stop()
  }

  def readConfig: ShopInfoConfig = {
    import net.ceedubs.ficus.Ficus._
    val configName = "shopInfo"
    ConfigUtil.readClassPathConfig[ShopInfoConfig](configName)
  }

  def fetchCityDF(implicit sqlContext: SQLContext): DataFrame = {
    val cityTB = "TBL_CHMGM_BUSS_ADMIN_DIVISION_CD"
    JdbcUtil.mysqlJdbcDF(cityTB)
  }

  def fetchAreaDF(implicit sqlContext: SQLContext): DataFrame = {
    val areaTB = "tbl_content_busi_area"
    //杭州商圈数据太脏 取cityCode前4位
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
      .selectExpr("trim(a.origin_name)  origin_name", "trim(b.industry_sub_nm)  industry_sub_nm",
        "trim(b.industry_no) industry_no", "trim(b.industry_sub_no)  industry_sub_no")
  }

  def fetchOnlineShopId(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val ts = Seq("04", "07", "08")
    val flatDF = JdbcUtil.mysqlJdbcDF("TBL_CHMGM_CHARA_GRP_DEF_FLAT").selectExpr("MCHNT_CD", "CHARA_GRP_CD")
    val couponDF = JdbcUtil.mysqlJdbcDF("TBL_CHMGM_TICKET_COUPON_INF")
      .filter("SALE_IN='1'")
      .filter($"BILL_TP".isin(ts: _*))
      .selectExpr("CHARA_GRP_CD", "SALE_IN", "BILL_TP")
    flatDF.as('a)
      .join(couponDF.as('b), $"a.CHARA_GRP_CD" === $"b.CHARA_GRP_CD", "leftsemi")
      .selectExpr("trim(a.MCHNT_CD)  MCHNT_CD")
  }

  def etl(args: Array[String])(implicit sqlContext: SQLContext) = {

    val config = readConfig
    //店铺编号生成
    sqlContext.udf.register("getShopNo", () => IdGenerator.generateShopID)
    //元转分
    sqlContext.udf.register("yuan2Fen", (yuan: javaBigDecimal) => yuan2cent(yuan))
    //获取营业时间
    sqlContext.udf.register("getShopTime", (time: String) => getShopTime(time))
    //获取停车状态
    sqlContext.udf.register("getServices", (park: String) => getServices(park))
    //获取商店名信息
    sqlContext.udf.register("getShopName", (shopName: String, brandName: String) => getShopName(shopName, brandName))

    val areaDF = fetchAreaDF
    val brandDF = fetchBrandDF
    val logicDF = fetchCategoryDf
    if (args(1) == "true") etlUnion(config, areaDF, logicDF)
    if (args(0) == "true") etlCrawl(config, areaDF, brandDF, logicDF)
  }

  def etlUnion(etlConfig: ShopInfoConfig, areaDF: DataFrame, logicDF: DataFrame)(implicit sqlContext: SQLContext) = {

    import sqlContext.implicits._
    val unionShopTB = "TBL_CHMGM_PREFERENTIAL_MCHNT_INF"
    //互联网
    val onlineShopIdDF = fetchOnlineShopId
    val onlineShopIds = onlineShopIdDF.map(_.getAs[String]("MCHNT_CD")).collect()
    //商户名带有 验证 测试  直接去掉
    val shopDF = JdbcUtil.mysqlJdbcDF(unionShopTB)
      .filter(!$"MCHNT_NM".contains("验证"))
      .filter(!$"MCHNT_NM".contains("测试"))

    //品牌信息
    val brandTb = "TBL_CHMGM_BRAND_INF"
    val brandDF = JdbcUtil.mysqlJdbcDF(brandTb)
      .selectExpr("BRAND_ID", "trim(BRAND_NM) BRAND_NM")

    val tDF = shopDF.as('a)
      .join(broadcast(brandDF).as('b), $"a.BRAND_ID" === $"b.BRAND_ID", "left_outer")
      .selectExpr("a.*", "coalesce(b.BRAND_NM,'')  BRAND_NM")

    val tmpDF = tDF
      .filter(!$"MCHNT_CD".isin(onlineShopIds: _*))
      .reNameColumn(etlConfig.union.reName)
      .appendDefaultVColumn(etlConfig.union.default)
      .selectExpr("SRC_SHOP_NO", "SRC_SHOP_TP", "getShopName(SHOP_NM,BRAND_NM)  SHOP_NM", "MCHNT_SECOND_PARA", "area_nm", "trim(PROV_CD) PROV_CD", "trim(CITY_CD) CITY_CD",
        "case when trim(COUNTY_CD)='' then trim(CITY_CD) else trim(COUNTY_CD) end COUNTY_CD", "SHOP_ADDR", "SHOP_LNT",
        "SHOP_LAT", "BUSI_TIME", "PARK_INF", "SHOP_IMAGE", "BRAND_NO", "SHOP_CONTACT_PHONE", "SHOP_ST", "MCHNT_ST", "SHOP_TP",
        "SHOP_VALID_DT_ST", "SHOP_VALID_DT_END", "SHOP_RATE", "SHOP_AVE_CONSUME")
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
      .join(broadcast(areaDF).as('d), $"a.area_nm" === $"d.area_nm" && $"a.COUNTY_CD" === $"d.county_cd", "left_outer")
      .selectExpr("a.*", "coalesce(d.area_no,'')  area_no", "coalesce(industry_no,'') industry_no", "coalesce(industry_sub_no,'') industry_sub_no")

    val df = finalDF.selectExpr("getShopNo() AS SHOP_NO", "SRC_SHOP_NO", "SRC_SHOP_TP", "SHOP_NM",
      "PROV_CD", "CITY_CD", "COUNTY_CD", "SHOP_ADDR", "case when SHOP_LNT=0 then '' else cast(SHOP_LNT as string) end SHOP_LNT",
      "case when SHOP_LAT=0 then '' else cast(SHOP_LAT as string) end SHOP_LAT",
      "case when length(BUSI_TIME)>60 then getShopTime(substring(BUSI_TIME,0,60)) else getShopTime(BUSI_TIME) end BUSI_TIME",
      "getServices(PARK_INF) AS SHOP_SERVICE", "SHOP_IMAGE",
      "area_no", "case when BRAND_NO=0 then ''  else cast(BRAND_NO as string) end BRAND_NO",
      "case when length(trim(SHOP_CONTACT_PHONE))>20 then '' else substring(trim(split(cast(SHOP_CONTACT_PHONE as string),',')[0]),0,20) end SHOP_CONTACT_PHONE", "SHOP_ST",
      "case when MCHNT_ST='2' then '1' else '0' end  SHOP_VALID_ST", "SHOP_TP", "SHOP_VALID_DT_ST", "SHOP_VALID_DT_END",
      "case when SHOP_RATE=0 then '' else cast(SHOP_RATE as string) end SHOP_RATE", "yuan2Fen(SHOP_AVE_CONSUME) SHOP_AVE_CONSUME",
      "industry_no", "industry_sub_no", "ROW_CRT_USR", "ROW_CRT_TS", "REC_UPD_USR", "REC_UPD_TS"
    )

    save2Mysql(df, "tbl_content_shop_inf")
    val onlineShopDF = shopDF
      .join(onlineShopIdDF, shopDF("MCHNT_CD") === onlineShopIdDF("MCHNT_CD"), "leftsemi")
      .selectExpr("getShopNo() AS SHOP_NO", "trim(MCHNT_CD) src_shop_no", "trim(MCHNT_NM) shop_nm",
        "case when length(trim(MCHNT_PHONE))>20 then '' else trim(split(cast(MCHNT_PHONE as string),',')[0]) end  shop_contact_phone",
        "'1' shop_valid_st", "COMMENT_VALUE shop_rate"
      ).addAlwaysColumn

    save2Mysql(onlineShopDF, "tbl_content_online_shop")

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
    val spBrand = brandName.replaceAll("[()（）]","")
    Option(spBrand) match {
      case None => shopName
      case Some(bb) => {
        if (spName.contains(bb)) spName else s"${bb}(${spName})"
      }
      case _ => spName
    }
  }

  def getShopTime(oHour: String) = {
    val name = oHour.replaceAll(" ", "").replaceAll("：", ":")
    val finalName = if (name == "24" || name == "二十四小时" || name.contains("24小时")) "00:00-23:59"
    else {
      val timeStr = timeRegex.findAllIn(name).toStream
      if (timeStr.isEmpty) {
        val dianStr = dianRegex.findAllIn(name).toStream
        if (dianStr.isEmpty || dianStr.size == 1) name
        else {
          dianStr.take(2).map(_.replaceAll("点", "")).toList match {
            case Nil => name
            case head :: second :: Nil => s"$head:00-${if (second.toInt > 10) second else second.toInt + 12}:00"
          }
        }
      }
      else {
        val mm = timeStr.sorted
        val min = mm.min
        val max = mm.max
        //if (min == max) s"$min-${max + 12}" else s"$min-$max"
        s"$min-$max"
      }
    }
    if (chRegex.findFirstIn(finalName).isDefined) "具体以店内公布为准" else finalName
  }

  def yuan2cent(y: javaBigDecimal) = {
    val rounded = y.setScale(2, RoundingMode.CEILING)
    val bigDecimalInCents = rounded.multiply(x100)
    bigDecimalInCents.intValueExact()
  }

  def etlCrawl(etlConfig: ShopInfoConfig, areaDF: DataFrame, brandDF: DataFrame, logicDF: DataFrame)(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    //店铺提供服务
    sqlContext.udf.register("hasServices", (tags: Seq[String]) => shopService(tags))
    //todo 数据有可能重复 见MongoUtil
    val mongoDF = sqlContext.fetchCrawlShopDF
    //去掉处理过的数据
    val tmpMongoDF = mongoDF
      //todo 匹配包含英文字母、中文的其他的过滤  杭州安规则来
      //      .filter($"shopName".rlike("""[\u4e00-\u9fa5a-zA-Z]"""))
      .selectExpr("_id.oid  src_shop_no", "shopName", "address", "case when score=0 then '' else cast(score as string) end score",
      "averagePrice", " case when length(trim(description))>2000 then substring(trim(description),0,2000) else trim(description) end description",
      "case when size(subCategory)=0 then '' else trim(subCategory[0]) end subCategory",
      "cast(city as string) city", "trim(tradingArea) tradingArea",
      "case when size(coordinates)=0 then '' else substring(cast(coordinates[0] as string),0,20) end shop_lnt",
      "case when size(coordinates)<2 then ''   else substring(cast(coordinates[1] as string),0,20) end shop_lat",
      "case when size(tags)=0 then 00000 else hasServices(tags) end shop_service", "trim(brand) brand_nm",
      "case when instr(pictureList[0],';')>0 then split(pictureList[0],';')[0] when size(pictureList)=0 then ''  else pictureList[0] end  shop_image",
      "case when instr(telephones[0],',')>0 then trim(split(telephones[0],',')[0]) when instr(telephones[0],' ')>0  then trim(split(telephones[0],' ')[0])  when size(telephones)=0 then '' else  substring(trim(telephones[0]),0,20) end  shop_contact_phone"
    )
    /* .groupBy("src_shop_no")
     .agg(first("shopName").as("shopName"), first("address").as("address"), first("score").as("score"), first("averagePrice").as("averagePrice"),
       first("description").as("description"), first("subCategory").as("subCategory"), first("city").as("city"), first("tradingArea").as("tradingArea"),
       first("shop_lnt").as("shop_lnt"), first("shop_lat").as("shop_lat"), first("shop_service").as("shop_service"), first("brand_nm").as("brand_nm"),
       first("shop_image").as("shop_image"), first("shop_contact_phone").as("shop_contact_phone")
     )
     .coalesce(6)*/


    val tmpDF = tmpMongoDF
      .reNameColumn(etlConfig.crawl.reName)
      .appendDefaultVColumn(etlConfig.crawl.default)
      .dealOpeningHours

    val finalDF = tmpDF.as('a)
      .join(logicDF.as('b), $"a.subCategory" === $"b.origin_name", "left_outer")
      .selectExpr("a.tradingArea", "a.city", "a.src_shop_no", "a.src_shop_tp", "a.shop_nm",
        "a.shop_lnt", "a.shop_lat", "a.busi_time", "a.shop_service", "a.shop_addr", "a.shop_st", "a.shop_valid_st",
        "a.shop_image", "a.shop_contact_phone", "a.brand_nm", "a.shop_tp", "a.shop_valid_dt_st", "a.shop_valid_dt_end",
        "a.shop_desc", "coalesce(b.industry_no,'')  industry_no", "coalesce(b.industry_sub_no,'') industry_sub_no",
        "a.shop_rate", "a.shop_ave_consume*100  shop_ave_consume"
      )

    val cityDF = fetchCityDF

    saveCrawlDB(finalDF, areaDF, brandDF, cityDF)

  }

  def saveCrawlDB(mongoDF: DataFrame, areaDF: DataFrame, brandDF: DataFrame, cityDF: DataFrame)(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    //todo 2000万数据在测试环境内存不够 广播join的相对较小的表、设大启动的executor内存
    val df = mongoDF.as('a)
      .join(broadcast(areaDF).as('b), $"a.tradingArea" === $"b.area_nm" and $"a.city" === $"b.county_cd", "left_outer")
      .join(broadcast(cityDF).as('d), $"a.city" === $"d.ADMIN_DIVISION_CD", "left_outer")
      .join(brandDF.as('c), $"a.brand_nm" === $"c.brand_nm", "left_outer")
      .selectExpr("getShopNo() as shop_no", "a.src_shop_no src_shop_no", "a.src_shop_tp", "a.shop_nm",
        "a.shop_lnt", "a.shop_lat", "a.busi_time", "a.shop_service", "a.shop_image", "coalesce(b.area_no,'')  area_no",
        "coalesce(c.brand_no,'')  brand_no", "a.shop_contact_phone", "a.shop_addr", "a.shop_st", "a.shop_valid_st",
        "a.shop_desc", "a.shop_tp", "a.shop_valid_dt_st", "a.shop_rate", "a.shop_ave_consume", "coalesce(d.PROV_DIVISION_CD,'') as prov_cd",
        "coalesce(d.CITY_DIVISION_CD,'') as city_cd", "coalesce(d.ADMIN_DIVISION_CD,'') as county_cd", "a.industry_no", "a.industry_sub_no"
        //        ,"a.ROW_CRT_USR", "a.ROW_CRT_TS", "a.REC_UPD_USR", "a.REC_UPD_TS"
      )
      .groupBy("src_shop_no")
      .agg(
        first("shop_no").as("shop_no"), first("src_shop_tp").as("src_shop_tp"), first("shop_nm").as("shop_nm"), first("shop_lnt").as("shop_lnt"),
        first("shop_lat").as("shop_lat"), first("busi_time").as("busi_time"), first("shop_service").as("shop_service"), first("shop_image").as("shop_image"),
        first("area_no").as("area_no"), first("brand_no").as("brand_no"), first("shop_contact_phone").as("shop_contact_phone"),
        first("shop_addr").as("shop_addr"), first("shop_st").as("shop_st"), first("shop_valid_st").as("shop_valid_st"),
        first("shop_desc").as("shop_desc"), first("shop_tp").as("shop_tp"), first("shop_valid_dt_st").as("shop_valid_dt_st"),
        first("shop_rate").as("shop_rate"), first("shop_ave_consume").as("shop_ave_consume"), first("prov_cd").as("prov_cd"),
        first("city_cd").as("city_cd"), first("county_cd").as("county_cd"), first("industry_no").as("industry_no"), first("industry_sub_no").as("industry_sub_no")
      )
      .addAlwaysColumn
      .coalesce(32)

    save2Mysql(df, "tbl_content_shop_inf")
  }

  def save2Mysql(df: DataFrame, table: String)(implicit sqlContext: SQLContext) = {
    JdbcUtil.save2Mysql(table)(df)
  }

  def shopService(tags: Seq[String]) = {
    val hasServices = serviceMap.filterKeys(tags.contains).map(_._2).toSeq
    val result = ds.map(s => {
      if (hasServices.contains(s)) 1
      else 0
    })
    result.mkString("")
  }

  def fetchIndustryLogic(implicit sqlContext: SQLContext): DataFrame = {
    JdbcUtil.mysqlJdbcDF("tbl_industry_etl_logic", "sink").selectExpr("type", "type_name", "origin_name", "real_name")
  }

}


//case class Crawl2ContentShopInfo(default: Map[String, String], reName: Map[String, String])
//
//case class Union2ContentShopInfo(default: Map[String, String], reName: Map[String, String])
//
//case class ShopInfoConfig(crawl: Crawl2ContentShopInfo, union: Union2ContentShopInfo)