package com.unionpay.test.online

import com.unionpay.db.jdbc.{JdbcSaveMode, JdbcUtil}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/11/30.
  */
object UpdateService {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("updateBrandNo")
      //      .setMaster("local[*]")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")

    sqlContext.udf.register("st", (st: String) => {
      val res = st.trim.replaceAll(" ", "")
      res match {
        case "0.0" => "0"
        case "1.0" => "1"
        case _ => res
      }
    })
    sqlContext.udf.register("parkInfo", (flashPay: String, free: String, wifi: String, park: String, card: String) => "" + flashPay + free + wifi + park + card)

    val serviceDF = sqlContext.read.parquet("/tmp/ls/result_tag")
      .selectExpr("MCHNT_CD src_shop_no", "st(flashPay) flashPay", "st(free) free", "st(wifi) wifi", "st(park) park", "st(card) card")
      .selectExpr("src_shop_no", "parkInfo(flashPay,free,wifi,park,card) shop_service")


    val shopDF = JdbcUtil.mysqlJdbcDF("tbl_content_shop_inf_20161129", "sink")
      .selectExpr("src_shop_no")

    val tmpDF = serviceDF.as('a)
      .join(shopDF.as('b), $"a.src_shop_no" === $"b.src_shop_no")
      .selectExpr("a.src_shop_no", "shop_service")

    JdbcUtil.saveToMysql("tbl_content_shop_inf_20161129", JdbcSaveMode.Upsert)(tmpDF)

    sc.stop()
  }

}
