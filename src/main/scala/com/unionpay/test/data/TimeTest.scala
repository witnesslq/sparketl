package com.unionpay.test.data

import java.io.PrintWriter

import com.unionpay.db.jdbc.JdbcUtil
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/10/17.
  */
object TimeTest {

  private lazy val timeRegex ="""\d{1,2}(:|：)\d{2}""".r
  private lazy val dianRegex ="""\d{1,2}点""".r
  private lazy val chRegex ="""[\u4e00-\u9fa5]""".r

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("InsertData to CMS === 插入数据")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")

    import sqlContext.implicits._

    val shopDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\export\\20161207\\tbl_content_shop_inf1206")
      .selectExpr("src_shop_no", "shop_valid_st", "brand_no")

    val shopAllDF = shopDF.selectExpr("src_shop_no", "brand_no")
      .groupBy("brand_no").agg(countDistinct("src_shop_no").as("shopCountAll"))

    val shopDF1 = shopDF.filter($"shop_valid_st" === "1")
      .selectExpr("src_shop_no", "brand_no")
      .groupBy("brand_no").agg(countDistinct("src_shop_no").as("shopCountSt1"))

    val brandDF1 = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\export\\20161207\\tbl_content_brand_inf")
      .filter(!$"brand_desc".contains("insert"))
      .selectExpr("brand_no", "src_brand_tp", "brand_nm", "brand_intro", "brand_desc")
      /*.map(r => {
        val brand_no = r.getAs[String]("brand_no")
        val src_brand_tp = r.getAs[String]("src_brand_tp")
        val brand_intro = r.getAs[String]("brand_intro")
        val brand_desc = r.getAs[String]("brand_desc").replaceAll("[\r|\n|\r\n|\\s+|\\t+]", "")
        (brand_no, src_brand_tp, brand_intro, brand_desc)
      }).toDF("brand_no", "src_brand_tp", "brand_intro", "brand_desc")*/

    val brandDF2 = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\export\\20161207\\23条数据")
      .drop("src_brand_no")
      /*.map(r => {
        val brand_no = r.getAs[String]("brand_no")
        val src_brand_tp = r.getAs[String]("src_brand_tp")
        val brand_intro = r.getAs[String]("brand_intro")
        val brand_desc = r.getAs[String]("brand_desc").replaceAll("[\r|\n|\r\n|\\s+|\\t+]", "")
        (brand_no, src_brand_tp, brand_intro, brand_desc)
      }).toDF("brand_no", "src_brand_tp", "brand_intro", "brand_desc")*/

    val brandDF = brandDF1.unionAll(brandDF2)

    //    val pw = new PrintWriter("C:\\Users\\ls\\Desktop\\export\\20161207\\brand_new.txt")
    //    pw.println("brand_no(品牌id),src_brand_tp(品牌来源),brand_nm(品牌名)," +
    //      "brand_intro(品牌简介),brand_desc(品牌介绍),shopCountAll(关联所有店铺数目),shopCountSt1(关联有效店铺数目),brand_valid_st(是否有效),relation_brand_no(相关联brand_no)")

    val df = brandDF.as('a)
      .join(shopAllDF.as('b), $"a.brand_no" === $"b.brand_no", "left_outer")
      .join(shopDF1.as('c), $"a.brand_no" === $"c.brand_no", "left_outer")
      .selectExpr("a.*", "coalesce(b.shopCountAll,'0') shopCountAll", "coalesce(c.shopCountSt1,'0') shopCountSt1")

    JdbcUtil.save2Mysql("export_table_brand_inf")(df)
    //    df.map(_.mkString(",")).collect.foreach(pw.println)
    //    pw.close()

    /*val logicDF = JdbcUtil.mysqlJdbcDF("tbl_brand_etl_logic", "sink")
      .selectExpr("brand_no", "brand_new_name")

    val brandDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\export\\20161207\\tbl_content_brand_inf")
      .filter($"src_brand_tp" === "1")
      .filter($"brand_desc".contains("insert"))
      .selectExpr("brand_no", "src_brand_tp", "brand_nm", "brand_intro")

    println(brandDF.count())

    val brandUnionDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\export\\20161207\\tbl_chmgm_brand_inf1207")
      .selectExpr("BRAND_ID", "BRAND_DESC")

    val tmpDF = brandDF.as('a)
      .join(logicDF.as('b), $"a.brand_nm" === $"b.brand_new_name", "left_outer")
      .selectExpr("a.*", "b.brand_no src_brand_no")

    val finalDF = tmpDF.as('a)
      .join(brandUnionDF.as('b), $"a.src_brand_no" === $"b.BRAND_ID", "left_outer")
      .selectExpr("a.*", "b.BRAND_DESC brand_desc")

    println(finalDF.count())
    println(finalDF.filter($"brand_desc".contains("insert")).count())

    finalDF.repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ls\\Desktop\\export\\20161207\\23条数据")*/

    sc.stop()
  }
}
