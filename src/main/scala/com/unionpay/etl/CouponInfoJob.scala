package com.unionpay.etl

import com.unionpay.db.jdbc.{JdbcUtil, MysqlConnection}
import com.unionpay.util.{ConfigUtil, IdGenerator}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import com.unionpay.db.mongo.MongoUtil._

/**
  * Created by ywp on 2016/7/13.
  */
object CouponInfoJob {

  def main(args: Array[String]) {
    if (args.size != 3) {
      System.err.println("useAge: crawl true or false unionpay true or false jobDateTime")
      System.exit(1)
    }
    val conf = new SparkConf()
      .setAppName("CouponInfoJob--优惠信息任务")
      //            .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "24")
    etl(args)
    sc.stop()
  }

  def readConfig: CouponConfig = {
    import net.ceedubs.ficus.Ficus._
    val configName = "coupon"
    ConfigUtil.readClassPathConfig[CouponConfig](configName)
  }

  def etl(args: Array[String])(implicit sqlContext: SQLContext) = {
    sqlContext.udf.register("genCouponId", () => IdGenerator.generateCouponID)
    sqlContext.udf.register("genGoodsId", () => IdGenerator.generateGoodsID)
    val config = readConfig
    if (args(0) == "true") etlCrawl(config)
    if (args(1) == "true") etlUnion(config)
  }

  def fetchUnionCouponDF(implicit sqlContext: SQLContext): DataFrame = {
    val mysql = MysqlConnection.build("mysql2mysql", "source")
    import mysql._
    val couponTicTB = "tbl_chmgm_ticket_coupon_inf"
    sqlContext.jdbcDF(couponTicTB)
  }

  def etlCrawl(config: CouponConfig)(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    //根据bankCode拆成多条数据
    //todo mongo数据一致性问题
    val mongoDF = sqlContext.fetchCrawlCouponDF
      .withColumn("coupon_launch_bank_no", explode($"bankCode")).drop($"bankCode")

    val tmpDF = mongoDF
      .reNameColumn(config.crawl.reName)
      .appendDefaultVColumn(config.crawl.default)
      .addAlwaysColumn
      .fixNotExistColumnException("coupon")

    //默认为空的值mysql自动赋值
    val finalDF = tmpDF.selectExpr("genCouponId() as coupon_no", "trim(src_coupon_no.oid) as src_coupon_no", "src_coupon_tp",
      "case when length(coupon_nm)>120 then substring(coupon_nm,0,120) else coupon_nm end coupon_nm",
      "case when size(limitDetails)=0 then ''  when length(limitDetails[0])>1024 then substring(limitDetails[0],0,1024)  else limitDetails[0]  end coupon_desc",
      "coupon_type", "coupon_launch_bank_no", "coalesce(coupon_img,'') as coupon_img", "case when size(pictureList)=0 then '' else  pictureList[0] end coupon_img_desc", "coupon_st", "coupon_tp",
      "case when startDate is null then '' when date_format(startDate,'yyyy')<'2000' then '' else date_format(startDate,'yyyyMMdd') end coupon_valid_dt_st", "case when endDate is null then '' when date_format(endDate,'yyyy')<'2000' then '' else  date_format(endDate,'yyyyMMdd') end coupon_valid_dt_end",
      "case when length(coupon_use_tm)>60 then substring(coupon_use_tm,0,60) else coupon_use_tm end coupon_use_tm", "coupon_card_require", "ROW_CRT_USR", "ROW_CRT_TS", "REC_UPD_USR", "REC_UPD_TS", "case when trim(coupon_rule)>800 then substring(trim(coupon_rule),0,800) else substring(trim(coupon_rule),0,800) end coupon_rule"
    )
    saveDB(finalDF, "tbl_content_coupon_inf")
  }

  def etlUnion(config: CouponConfig)(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._

    val couponDF = fetchUnionCouponDF
    val onlineGoodsDF = couponDF
      .filter("SALE_IN='1'")
      //todo 加上逻辑
      .filter($"BILL_TP".isin(Seq("01", "02", "03", "04", "07", "09", "10"): _*))

    val onlineGoodsId = onlineGoodsDF.selectExpr("BILL_ID").map(_.getAs[String]("BILL_ID")).collect()

    val tmp = couponDF
      .filter(!$"BILL_ID".isin(onlineGoodsId: _*))
      .reNameColumn(config.union.reName)
      .appendDefaultVColumn(config.union.default)
      .addAlwaysColumn

    val finalDF = tmp.selectExpr("genCouponId() as coupon_no", "trim(SRC_COUPON_NO) as SRC_COUPON_NO", "SRC_COUPON_TP", "COUPON_NM", "COUPON_DESC",
      "COUPON_TYPE",
      "case when BILL_TP in ('01','02','03') then concat('https://youhui.95516.com/app/image/mchnt/coupon/',trim(SRC_COUPON_NO),'_logo_list_app.jpg') " +
        "else concat('https://youhui.95516.com/app/image/mchnt/ticket/',trim(SRC_COUPON_NO),'_logo_list_app.jpg') end COUPON_IMG",
      //      "concat('https://youhui.95516.com/app/image/mchnt/coupon/',trim(SRC_COUPON_NO),'_logo_list_app.jpg') as COUPON_IMG", "case when BILL_ST='2' then 1 else 0 end COUPON_ST",
      "COUPON_TP", "COUPON_VALID_DT_ST", "COUPON_VALID_DT_END", "COUPON_CARD_REQUIRE", "COUPON_LAUNCH_BANK_NO",
      "case when trim(COUPON_ORDER_REMIND)>200 then substring(trim(COUPON_ORDER_REMIND),0,200) else substring(trim(COUPON_ORDER_REMIND),0,200) end COUPON_ORDER_REMIND",
      "case when trim(COUPON_RULE_REMIND)>400 then substring(trim(COUPON_RULE_REMIND),0,400) else substring(trim(COUPON_RULE_REMIND),0,400) end COUPON_RULE_REMIND",
      "case when trim(COUPON_RULE)>800 then substring(trim(COUPON_RULE),0,800) else substring(trim(COUPON_RULE),0,800) end COUPON_RULE",
      "ROW_CRT_USR", "ROW_CRT_TS", "REC_UPD_USR", "REC_UPD_TS"
    )

    saveDB(finalDF, "tbl_content_coupon_inf")

    val goodsDF = onlineGoodsDF.selectExpr("genGoodsId() as goods_no", "trim(BILL_ID) src_goods_no", "BILL_NM goods_name",
      "case when BILL_TP in ('01','02','03') then concat('https://youhui.95516.com/app/image/mchnt/coupon/',trim(BILL_ID),'_logo_list_app.jpg') " +
        "else concat('https://youhui.95516.com/app/image/mchnt/ticket/',trim(BILL_ID),'_logo_list_app.jpg') end goods_img",
      //      "concat('https://youhui.95516.com/app/image/mchnt/coupon/',trim(BILL_ID),'_logo_list_app.jpg') as goods_img",
      "case when trim(PREFERENTIAL_COND)>200 then substring(trim(PREFERENTIAL_COND),0,200) else substring(trim(PREFERENTIAL_COND),0,200) end  goods_intro",
      "case when trim(BILL_DESC)>200 then substring(trim(BILL_DESC),0,200) else substring(trim(BILL_DESC),0,200) end goods_desc", "'1' goods_state",
      "BILL_ORIGINAL_PRICE unit_price_origin", "BILL_PRICE unit_price_sale", "'00010000' launch_bank_no",
      "case when trim(BILL_DESC)>200 then substring(trim(BILL_DESC),0,200) else substring(trim(BILL_DESC),0,200) end rule_remind", "'B0' industry_no",
      "VALID_BEGIN_DT goods_valid_dt_st", "VALID_END_DT goods_valid_dt_end"
    ).addAlwaysColumn

    saveDB(goodsDF, "tbl_content_goods_inf")

  }

  def saveDB(df: DataFrame, table: String)(implicit sqlContext: SQLContext) = {
    JdbcUtil.save2Mysql(table)(df)
  }

}

case class CrawlCouponConfig(reName: Map[String, String], default: Map[String, String])

case class UnionCouponConfig(reName: Map[String, String], default: Map[String, String])

case class CouponConfig(crawl: CrawlCouponConfig, union: UnionCouponConfig)