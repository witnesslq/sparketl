package com.unionpay.etl

import com.unionpay.db.jdbc.JdbcUtil
import com.unionpay.util.IdGenerator
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * todo 评论太多换银联自有逻辑
  * 依赖tbl_content_shop_inf tbl_content_relate_shop_coupon
  * Created by ywp on 2016/7/8.
  */
object CommentJob {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("CommentJob--评论信息任务")
      //      .setMaster("local[4]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")
    etl
    sc.stop()
  }

  def fetchBrandCommentDF(implicit sQLContext: SQLContext): DataFrame = {
    val commentTB = "TBL_CHMGM_BRAND_COMMENT_INF"
    JdbcUtil.mysqlJdbcDF(commentTB).selectExpr("BRAND_ID", "BRAND_COMMENT_TXT", "CDHD_USR_ID")
  }

  def etl(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    //店铺编号生成
    val getCommentNo = udf(() => IdGenerator.generateCommentID)
    val commentTB = "tbl_content_comment"
    val shopTB = "tbl_content_shop_inf"
    val shopCouponTB = "tbl_content_relate_shop_coupon"
    val brandCommentDF = fetchBrandCommentDF
    val shopDF = JdbcUtil.mysqlJdbcDF(shopTB, "sink").filter($"src_shop_tp" === "1").selectExpr("shop_no", "brand_no")
    val shopCouponDF = JdbcUtil.mysqlJdbcDF(shopCouponTB, "sink").selectExpr("coupon_no", "shop_no")
    //todo 每个店铺只取一个优惠
    val tmpShopCouponDF = shopCouponDF.groupBy($"shop_no").agg(first($"coupon_no").as("coupon_no"))
    val finalDF = shopDF.as('a)
      .join(brandCommentDF.as('b), $"a.brand_no" === $"b.BRAND_ID")
      .join(tmpShopCouponDF.as('c), $"a.shop_no" === $"c.shop_no")
      .selectExpr("a.shop_no", "b.BRAND_COMMENT_TXT as content", "b.CDHD_USR_ID as comment_user_id", "c.coupon_no")
      .groupBy($"shop_no", $"coupon_no", $"comment_user_id").agg(first($"content").as("content"))
      .addAlwaysColumn
      .withColumn("comment_no", getCommentNo())
    JdbcUtil.save2Mysql(commentTB)(finalDF)
  }


}
