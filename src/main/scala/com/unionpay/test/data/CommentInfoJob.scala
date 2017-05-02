package com.unionpay.test.data

import com.unionpay._
import com.unionpay.etl._
import com.unionpay.db.jdbc.{JdbcSaveMode, JdbcUtil}
import com.unionpay.util.IdGenerator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/10/21.
  */
object CommentInfoJob {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("CommentJob--评论信息任务")
      //      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", spark_shuffle_partitions)
    etl(args)
    sc.stop()
  }

  def fetchBrandCommentDF(implicit sQLContext: SQLContext): DataFrame = {
    val commentTB = "TBL_CHMGM_BRAND_COMMENT_INF"
    JdbcUtil.mysqlJdbcDF(commentTB)
      .selectExpr("BRAND_ID", "BRAND_COMMENT_TXT", "CDHD_USR_ID")
  }

  def etl(args: Array[String])(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._

    val getCommentNo = udf(() => IdGenerator.generateCommentID)
    val commentTB = "tbl_content_comment"
    val shopTB = "tbl_content_shop_inf"
    val brandTB = "tbl_content_brand_inf"
    val shopCouponTB = "tbl_content_relate_shop_coupon"
    val shopTmpBrand = "tbl_chmgm_shop_tmp"
    val brandCommentDF = fetchBrandCommentDF
    val shopDF = JdbcUtil.mysqlJdbcDF(shopTB, "sink").filter($"src_shop_tp" === "1").selectExpr("shop_no", "brand_no")
    val brandDF = JdbcUtil.mysqlJdbcDF(brandTB, "sink").selectExpr("brand_no", "src_brand_no")
    val brandLogicDF = JdbcUtil.mysqlJdbcDF(shopTmpBrand, "sink").selectExpr("BRAND_ID old_brand", "transId new_brand").distinct()

    val tmpBrandDF = brandDF.as('a)
      .join(brandLogicDF.as('b), $"a.src_brand_no" === $"b.new_brand", "left_outer")
      .selectExpr("a.brand_no brand_no", "b.old_brand src_brand_no")
      .groupBy("src_brand_no")
      .agg(first("brand_no").as("brand_no"))

    val shopBrandDF = shopDF.as('a)
      .join(tmpBrandDF.as('b), $"a.brand_no" === $"b.brand_no", "left_outer")
      .selectExpr("a.shop_no shop_no", "b.src_brand_no brand_no")
    val shopCouponDF = JdbcUtil.mysqlJdbcDF(shopCouponTB, "sink").selectExpr("coupon_no", "shop_no")
    //todo 每个店铺只取一个优惠
    val tmpShopCouponDF = shopCouponDF.groupBy($"shop_no").agg(first($"coupon_no").as("coupon_no"))
    val finalDF = shopBrandDF.as('a)
      .join(brandCommentDF.as('b), $"a.brand_no" === $"b.BRAND_ID")
      .join(tmpShopCouponDF.as('c), $"a.shop_no" === $"c.shop_no")
      .selectExpr("a.shop_no", "b.BRAND_COMMENT_TXT as content", "b.CDHD_USR_ID as comment_user_id", "c.coupon_no")
      .groupBy($"shop_no", $"coupon_no", $"comment_user_id").agg(first($"content").as("content"))
      .addAlwaysColumn
      .withColumn("comment_no", getCommentNo())
    JdbcUtil.saveToMysql(commentTB, JdbcSaveMode.Upsert)(finalDF)
  }

}
