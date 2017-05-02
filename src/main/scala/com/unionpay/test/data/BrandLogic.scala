package com.unionpay.test.data

import java.io.PrintWriter

import com.unionpay.db.jdbc.{JdbcSaveMode, JdbcUtil}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/11/28.
  */
object BrandLogic {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("BrandLogic === 获取品牌逻辑信息")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "4")

    val bn = sqlContext.udf.register("bn", (o: String, n: String) => {
      val on = o.trim.replaceAll(" ", "")
      val nn = n.trim.replaceAll(" ", "") match {
        case "2" | "2.0" => ""
        case s: String => s
      }
      if (nn.isEmpty) on else nn
    })

    val shopDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\result_brand")
      .selectExpr("shopNo", "bn(trim(oldBrandName),trim(newBrandName)) brand_name")

    val brandDF = JdbcUtil.mysqlJdbcDF("tbl_content_brand_inf", "sink")

    val ds = shopDF.as('a)
      .join(brandDF.as('b), $"a.brand_name" === $"b.brand_nm")
      .selectExpr("a.shopNo src_shop_no", "b.brand_no brand_no")
      .groupBy("src_shop_no")
      .agg(first("brand_no").as("brand_no"))
      .map(r => {
        val s = r.getAs[String]("src_shop_no")
        val b = r.getAs[String]("brand_no")
        s"update tbl_content_shop_inf set brand_no='$b' where src_shop_no='$s';"
      }).collect()

    writeSql("brand", ds)


    sc.stop()
  }

  def writeSql(name: String, ds: Seq[String]) = {
    val pw = new PrintWriter(s"C:\\Users\\ls\\Desktop\\Desktop\\$name.sql")
    ds.foreach(pw.println)
    pw.close()
  }
}
