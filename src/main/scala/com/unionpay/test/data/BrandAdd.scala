package com.unionpay.test.data

import java.util.UUID

import com.unionpay.db.jdbc.JdbcUtil
import com.unionpay.util.IdGenerator
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

import scala.io.Source

/**
  * Created by ls on 2016/10/14.
  */
object BrandAdd {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("BrandLogic === 获取品牌逻辑信息")
      //      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "4")
    val generateBrandID = udf(() => s"${UUID.randomUUID.toString.replaceAll("-", "")}")

    val getBrandNo = udf(() => IdGenerator.generateBrandID)
    val bn = sqlContext.udf.register("bn", (o: String, n: String) => {
      val on = o.trim.replaceAll(" ", "")
      val nn = n.trim.replaceAll(" ", "") match {
        case "2" | "2.0" => ""
        case s: String => s
      }
      if (nn.isEmpty) on else nn
    })

    val contentBrandDF = sqlContext.read.parquet("/tmp/ls/tbl_content_brand_inf")
    val brandIds = contentBrandDF.selectExpr("brand_nm")
      .distinct()
      .map(_.getAs[String]("brand_nm"))
      .collect()

    val brandDF = sqlContext.read.parquet("/tmp/ls/result_brand")
      .selectExpr("C49 brand_no", "bn(trim(oldBrandName),trim(newBrandName)) brand_name")
      .distinct()
      .filter(!($"brand_name".isin(brandIds: _*)))


    val tmpDF = brandDF.as('a)
      .join(contentBrandDF.drop("brand_nm").as('b), $"a.brand_no" === $"b.src_brand_no")
      .selectExpr("a.brand_name brand_nm", "b.*")
      .drop("brand_no")
      .drop("src_brand_no")
      .drop("row_id")
      .withColumn("brand_no", getBrandNo())
      .withColumn("src_brand_no", generateBrandID())
      .withColumn("row_crt_ts", lit("2016-11-10 23:59:59"))
      .withColumn("rec_upd_ts", lit("2016-11-10 23:59:59"))
    //      .withColumn("src_brand_tp", lit(1))

    JdbcUtil.save2Mysql("tbl_content_brand_inf_sb_tmp")(tmpDF)
    sc.stop()
  }
}
