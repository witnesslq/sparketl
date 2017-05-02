package com.unionpay.test.data

import com.unionpay.db.jdbc.JdbcUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/11/1.
  */
object DealShopBrandName {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("UpdateShopName to CMS === 更新店铺名数据")
      //      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "32")
    import sqlContext.implicits._

    sqlContext.udf.register("getShopName", (shopName: String, brandName: String) => getShopName(shopName, brandName))
    sqlContext.udf.register("unionValue", (value: String, new_value: String) => unionValue(value, new_value))

    val shopDF = JdbcUtil.mysqlJdbcDF("tbl_content_shop_inf", "sink")
      .selectExpr("src_shop_no", "shop_nm")

    val shopTmpDF = JdbcUtil.mysqlJdbcDF("tbl_chmgm_shop_tmp", "sink")
      .filter(!$"MCHNT_NM".contains("验证"))
      .filter(!$"MCHNT_NM".contains("测试"))
      .filter($"brandId" !== $"trandsId ")
      .selectExpr("MCHNT_CD", "")


    sc.stop()
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

}
