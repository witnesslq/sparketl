package com.unionpay.test.data

import com.unionpay.db.jdbc.{JdbcSaveMode, JdbcUtil}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/11/1.
  */
object DealShopName {

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


    val shopOrDF = JdbcUtil.mysqlJdbcDF("tbl_chmgm_shop_tmp", "sink")
      .filter(!$"MCHNT_NM".contains("验证"))
      .filter(!$"MCHNT_NM".contains("测试"))
      .selectExpr("MCHNT_CD", "unionValue(MCHNT_NM,NEW_NAME) SHOP_NM")


    val shopDF = JdbcUtil.mysqlJdbcDF("tbl_content_shop_inf", "sink")
      .filter($"shop_nm".contains("(2.0)"))
      .selectExpr("src_shop_no", "brand_no")

    println("shopDF个数：" + shopDF.count())

    val brandTb = "tbl_content_brand_inf"
    val brandDF = JdbcUtil.mysqlJdbcDF(brandTb, "sink")
      .selectExpr("brand_no", "trim(brand_nm) brand_nm")

    val tDF = shopDF.as('a)
      .join(brandDF.as('b), $"a.brand_no" === $"b.brand_no", "left_outer")
      .selectExpr("a.src_shop_no", "coalesce(b.brand_nm,'')  brand_nm")

    val df = tDF.as('a)
      .join(shopOrDF.as('b), $"a.src_shop_no" === $"b.MCHNT_CD", "left_outer")
      .selectExpr("a.src_shop_no", "b.SHOP_NM shop_nm")

    df.printSchema()
    println(df.count())
    df.show(10)
    JdbcUtil.saveToMysql("tbl_content_shop_inf", JdbcSaveMode.Upsert)(df)

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
