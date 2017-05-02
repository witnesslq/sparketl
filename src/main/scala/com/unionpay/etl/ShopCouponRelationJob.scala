package com.unionpay.etl

import com.unionpay.db.jdbc.JdbcUtil
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import com.unionpay.db.mongo.MongoUtil._
import org.apache.spark.storage.StorageLevel

/**
  * 依赖 tbl_content_shop_inf  tbl_content_coupon_inf
  * Created by ywp on 2016/7/8.
  */
object ShopCouponRelationJob {

  def main(args: Array[String]) {

    if (args.size != 3) {
      System.err.println("useAge: crawl true or false unionpay true or false jobDateTime")
      System.exit(1)
    }

    val conf = new SparkConf()
      .setAppName("ShopCouponRelationJob--商户优惠关系任务")
      //      .setMaster("local[4]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.yarn.driver.memoryOverhead", "1024")
      .set("spark.yarn.executor.memoryOverhead", "2000")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")
    etl(args)
    sc.stop()
  }

  def etl(args: Array[String])(implicit sqlContext: SQLContext) = {
    val shopCouponRelationTB = "tbl_content_relate_shop_coupon"
    val srcCouponDF = fetchCouponDF.persist(StorageLevel.MEMORY_AND_DISK)
    val srcShopDF = fetchShopDF.persist(StorageLevel.MEMORY_AND_DISK)
    if (args(1) == "true") etlUnion(shopCouponRelationTB, srcCouponDF, srcShopDF)
    if (args(0) == "true") etlCrawl(shopCouponRelationTB, srcCouponDF, srcShopDF)
  }

  def fetchCouponDF(implicit sqlContext: SQLContext): DataFrame = {
    val couponTB = "tbl_content_coupon_inf"
    JdbcUtil.mysqlJdbcDF(couponTB, "sink").selectExpr("src_coupon_tp", "coupon_no", "src_coupon_no")
  }

  def fetchShopDF(implicit sqlContext: SQLContext): DataFrame = {
    val shopTB = "tbl_content_shop_inf"
    JdbcUtil.mysqlJdbcDF(shopTB, "sink").selectExpr("src_shop_tp", "shop_no", "src_shop_no")
  }

  def etlCrawl(shopCouponRelationTB: String, srcCouponDF: DataFrame, srcShopDF: DataFrame)
              (implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    val mongoDF = sqlContext.fetchCrawlCouponDF.dealShopList
    val couponDF = srcCouponDF.filter($"src_coupon_tp" === "0")
    val couponShopDF = couponDF.as('a)
      .join(mongoDF.as('b), $"a.src_coupon_no" === $"b._id.oid")
      .selectExpr("b.src_shop_no", "a.coupon_no")
    val shopDF = srcShopDF.filter($"src_shop_tp" === "0")
    val finalDF = shopDF.as('s)
      .join(broadcast(couponShopDF).as('cs), $"cs.src_shop_no" === $"s.src_shop_no")
      .selectExpr("cs.coupon_no", "s.shop_no")
      .addAlwaysColumn
    JdbcUtil.save2Mysql(shopCouponRelationTB)(finalDF)
  }

  def etlUnion(shopCouponRelationTB: String, srcCouponDF: DataFrame, srcShopDF: DataFrame)
              (implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._

    val couponDF = srcCouponDF.filter($"src_coupon_tp" === "1")
    val unionCouponTB = "tbl_chmgm_ticket_coupon_inf"
    val unionFlatTB = "tbl_chmgm_chara_grp_def_flat"

    val goodsDF = JdbcUtil.mysqlJdbcDF("tbl_content_goods_inf", "sink").selectExpr("goods_no", "src_goods_no")

    val billIds = goodsDF.map(_.getAs[String]("src_goods_no")).collect()

    val originCouponDF = JdbcUtil.mysqlJdbcDF(unionCouponTB)

    val unionCouponDF = originCouponDF.filter(!$"BILL_ID".isin(billIds: _*)).selectExpr("BILL_ID", "CHARA_GRP_CD")

    val unionFlatDF = JdbcUtil.mysqlJdbcDF(unionFlatTB).selectExpr("CHARA_GRP_CD", "MCHNT_CD")

    val unionCouponFlatDF = unionCouponDF
      .join(unionFlatDF, unionCouponDF("CHARA_GRP_CD") === unionFlatDF("CHARA_GRP_CD"))
      .select(unionCouponDF("BILL_ID"), unionFlatDF("MCHNT_CD"))

    val cpDF = couponDF.as('a)
      .join(unionCouponFlatDF.as('b), $"a.src_coupon_no" === $"b.BILL_ID")
      .selectExpr("a.coupon_no", "b.MCHNT_CD as src_shop_no")

    val shopDF = srcShopDF.filter($"src_shop_tp" === "1")

    val finalDF = shopDF.as('d)
      .join(broadcast(cpDF).as('c), $"c.src_shop_no" === $"d.src_shop_no")
      .selectExpr("c.coupon_no", "d.shop_no")
      .addAlwaysColumn

    JdbcUtil.save2Mysql(shopCouponRelationTB)(finalDF)

    val goodsCouponDF = originCouponDF.join(goodsDF, originCouponDF("BILL_ID") === goodsDF("src_goods_no"), "leftsemi")
      .selectExpr("BILL_ID", "CHARA_GRP_CD")

    val unionGoodsFlatDF = goodsCouponDF
      .join(unionFlatDF, goodsCouponDF("CHARA_GRP_CD") === unionFlatDF("CHARA_GRP_CD"))
      .select(unionCouponDF("BILL_ID"), unionFlatDF("MCHNT_CD"))

    val onlineShopDF = JdbcUtil.mysqlJdbcDF("tbl_content_online_shop", "sink").selectExpr("shop_no", "src_shop_no")

    val goodsBillDF = onlineShopDF.as('a)
      .join(unionGoodsFlatDF.as('b), $"a.src_shop_no" === $"b.MCHNT_CD")
      .selectExpr("a.shop_no", "b.BILL_ID")

    val finalRelationDF = goodsDF.as('a)
      .join(goodsBillDF.as('b), $"a.src_goods_no" === $"b.BILL_ID")
      .selectExpr("a.goods_no", "b.shop_no")
      .addAlwaysColumn

    JdbcUtil.save2Mysql("tbl_content_relate_goods_onlineshop")(finalRelationDF)

  }
}
