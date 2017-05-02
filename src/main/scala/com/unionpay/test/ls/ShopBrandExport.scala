package com.unionpay.test.ls

import java.io.PrintWriter

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/11/29.
  */
object ShopBrandExport {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("生成shopCouponSql语句")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "4")
    import sqlContext.implicits._


    val brandDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\sl\\data29\\tbl_content_brand_inf29")
      .selectExpr("brand_no", "brand_nm")
      .filter($"brand_nm" === "爱茜茜里")
      .filter($"brand_no" === "BRND972096a2fcfe47aa949b1bbaadfcd215")

    //        val shopIds = Seq(
    //          "SHOP6d20b08c465a4229a90316cf4c9ec0f5",
    //          "SHOP70373a8e3f1041d086324e1eaee2f0a2",
    //          "SHOP2a274100ba624f3ca27002f234031296",
    //          "SHOP9f8c881164aa4adb8fb0b561281bdcd4"
    //        )
    val shopDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\sl\\data29\\tbl_content_shop_inf29")
      .filter($"shop_valid_st" === 1)
      .selectExpr("shop_no", "shop_nm", "brand_no", "shop_valid_st")
    //      .filter($"shop_no".isin(shopIds: _*))

    shopDF.map(_.mkString("[", "|", "]"))
      .collect()
      .foreach(println)

    brandDF.as('a)
      .join(shopDF.as('b), $"a.brand_no" === $"b.brand_no")
      .selectExpr("a.brand_no", "b.shop_no")
      .groupBy('brand_no)
      .agg(collect_set("shop_no").as("shopIds"))
      .map(_.mkString("[", "|", "]"))
      //      .count()
      //      .map(_.mkString(","))
      .collect()
      .foreach(println)

    sc.stop()
  }

}
