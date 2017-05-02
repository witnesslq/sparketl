package com.unionpay.test.data

import java.util.UUID

import com.unionpay.db.jdbc.{JdbcSaveMode, JdbcUtil}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/10/19.
  */
object BrandNewInfo {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("BrandNewInfo === 生成新品牌信息")
      //      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "32")
    val generateBrandID = udf(() => s"${UUID.randomUUID.getLeastSignificantBits.toString.replaceAll("-", "")}")

    val brandLogicDF = JdbcUtil.mysqlJdbcDF("tbl_brand_etl_logic_bak", "sink")
    val brandDF = JdbcUtil.mysqlJdbcDF("tbl_chmgm_brand_inf")

    val brandNameDF = brandLogicDF.selectExpr("brand_no", "brand_new_name")
      .groupBy("brand_new_name")
      .agg(first("brand_no").as("src_brand_no"))

    val tmpDF = brandNameDF.as('a)
      .join(brandDF.as('b), $"a.src_brand_no" === $"b.BRAND_ID", "left_outer")
      .selectExpr("b.*", "a.brand_new_name").drop("BRAND_NM")
      .withColumn("BRAND_ID", generateBrandID())

    val fields = tmpDF.schema.fieldNames

    val exprs = fields.map(cloumn => {
      cloumn match {
        case "BUSS_BMP" | "CUP_BRANCH_INS_ID_CD" | "BRAND_DESC" | "BRAND_ST" | "BRAND_TP" | "ENTRY_INS_ID_CD" | "ENTRY_INS_CN_NM" | "REC_CRT_USR_ID" | "REC_UPD_USR_ID" => s"coalesce($cloumn,'') $cloumn"
        case "AVG_CONSUME" | "AVG_COMMENT" | "CONTENT_ID" | "BRAND_ENV_GRADE" | "BRAND_SRV_GRADE" | "BRAND_POPULAR_GRADE" | "BRAND_TASTE_GRADE" => s"coalesce(${cloumn},0) $cloumn"
        case _: String => cloumn
      }
    })
    val df = tmpDF.selectExpr(exprs: _*)
      .withColumnRenamed("brand_new_name", "BRAND_NM")

    df.printSchema()
    df.show()
    JdbcUtil.saveToMysql("tbl_chmgm_brand_tmp_bak20161128", JdbcSaveMode.Upsert)(df)

    sc.stop()

  }
}
