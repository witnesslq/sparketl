package com.unionpay.test

import java.io.PrintWriter

import com.unionpay.db.jdbc.JdbcUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ywp on 2016/8/1.
  */
object ExportCategoryArea {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("ExportCategoryArea--商户数据所属分类、商圈分别去重")
    //      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")
    val stopWords = Seq("专享", "银联", "券", "观影", "银行", "邮储", "权益", "IC卡", "优惠", "测试", "扫码", "云闪付", "积分", "活动", "重阳", "62", "悦享", "测试", "约惠星期六")
    val brandDF = JdbcUtil.mysqlJdbcDF("tbl_chmgm_brand_inf", "source")
      .filter(!$"BRAND_NM".rlike("""[0-9]+[元减 减 立减].?"""))
      .filter(!$"BRAND_NM".rlike("""[0-9]+折"""))
      .filter(!$"BRAND_NM".rlike("""^-?[1-9]\d*$"""))
      .filter(!$"BRAND_NM".isin(stopWords: _*))
      .filter("length(trim(BRAND_NM))>0")
      .selectExpr("BRAND_NM")
      .orderBy(length($"BRAND_NM").desc)
    val brand = "/tmp/brand_name_delete_sp.csv"
    val pw = new PrintWriter(brand, "UTF-8")
    brandDF
      .map(_.getAs[String]("BRAND_NM"))
      .collect()
      .foreach(pw.println)
    pw.close()

    sc.stop()
  }


}
