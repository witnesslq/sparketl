package com.unionpay.test.online

import java.io.PrintWriter

import com.unionpay._
import com.unionpay.db.jdbc.{JdbcSaveMode, JdbcUtil}
import com.unionpay.db.mongo.MongoUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/12/1.
  */
object DelShopTime {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("shopInfoJob--店铺信息任务")
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
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", spark_shuffle_partitions)

    /*val shopIds = JdbcUtil.mysqlJdbcDF("tbl_content_shop_inf_20161201", "sink")
      .selectExpr("src_shop_no")
      .map(_.getAs[String]("src_shop_no"))
      .collect()

    val logicDF = fetchCategoryDF

    val mongoDF = sqlContext.fetchCrawlShopDF
      .filter($"_id.oid".isin(shopIds: _*))
      .selectExpr("_id.oid src_shop_no", "case when size(subCategory)=0 then '' else trim(subCategory[0]) end subCategory")


    val finalDF = mongoDF.as('a)
      .join(logicDF.as('b), $"a.subCategory" === $"b.real_name", "left_outer")
      .selectExpr("a.src_shop_no", "coalesce(b.industry_no,'')  industry_no", "coalesce(b.industry_sub_no,'') industry_sub_no")

    JdbcUtil.saveToMysql("tbl_content_shop_inf_20161201", JdbcSaveMode.Upsert)(finalDF)*/

    /*val update = JdbcUtil.mysqlJdbcDF("tbl_content_shop_inf_20161201", "sink")
      .selectExpr("src_shop_no", "shop_image", "shop_image_desc", "industry_no", "industry_sub_no")
      .map(r => {
        val ss = r.getAs[String]("src_shop_no")
        val si = r.getAs[String]("shop_image")
        val sid = r.getAs[String]("shop_image_desc")
        val in = r.getAs[String]("industry_no")
        val isn = r.getAs[String]("industry_sub_no")
        s"update tbl_content_shop_inf set shop_image='$si',shop_image_desc='$sid',industry_no='$in',industry_sub_no='$isn' where src_shop_no='$ss';"
      }).collect()

    def writeSql(name: String, ds: Seq[String]) = {
      val pw = new PrintWriter(s"/tmp/ls/$name.sql")
      ds.foreach(pw.println)
      pw.close()
    }
    writeSql("update_industry", update)*/

    /*        sqlContext.udf.register("delTime", (time: String) => {

              val xx = if (time.getBytes().length != time.length()
                && (time.contains("24小时") || time.contains("二十四小时") || time.equals("每天")))
                "00:00-23:59"
              else time

              xx match {
                case null | "" => "具体以店内公布为准"
                case _ => {
                  val pp = xx.replaceAll("[\u4e00-\u9fa5]+", "")
                  pp match {
                    case "24" => "00:00-23:59"
                    case _ => xx.replaceAll("[am|pm|每天|每日|AM|PM|周一至周日]", "")
                  }
                }
              }
            })


            val shopDF = JdbcUtil.mysqlJdbcDF("tbl_content_shop_inf_20161201", "sink")
              .selectExpr("src_shop_no", "delTime(busi_time) busi_time")

            JdbcUtil.saveToMysql("tbl_content_shop_inf_20161201", JdbcSaveMode.Upsert)(shopDF)*/

    sc.stop()
  }


  def fetchSubIndustryDF(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val subTb = "tbl_content_industry_sub_inf"
    JdbcUtil.mysqlJdbcDF(subTb, "sink").selectExpr("industry_no", "industry_sub_no", "industry_sub_nm")
  }

  def fetchCategoryDF(implicit sqlContext: SQLContext): DataFrame = {
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
