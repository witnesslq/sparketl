package com.unionpay.test

import com.unionpay.db.jdbc.JdbcUtil
import com.unionpay.db.mongo.MongoUtil._
import com.unionpay.util.IdGenerator
import com.unionpay.util.NLP._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ywp on 2016/8/10.
  */
object Classify {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Classify")
    //      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")
    /*sqlContext.udf.register("removeSW", (tx: String) => {
      val ws = tx.wordsNameNature
      val flag = if (ws.size == 1 && ws.map(_._2).contains("w")) 0
      else if (tx.trim.size == 1) 0
      else if (tx.trim.isEmpty) 0
      else 1
      flag
    })
    val (brandInfoDF, crawlDF) = distinctBrandName
    brandInfoDF
      .withColumn("tt", lit("hello"))
      .show()*/
    val pa = udf((id: BigInt) => id.hashCode() % 3)
    val shopDF = JdbcUtil.mysqlJdbcDF("tbl_content_shop_inf", "sink").filter("src_shop_tp='0'")
      .repartition(3, pa($"row_id"))
    val ductDF = shopDF.groupBy($"src_shop_no").agg(count($"row_id").as("total")).filter("total>1")
    ductDF.write.mode(SaveMode.Overwrite).parquet("/test/ducShop")
    //    val caDF = shopDF.as('a)
    //      .join(ductDF.as('b), $"a.src_shop_no" === $"b.src_shop_no", "leftsemi")
    sc.stop()
  }

  def distinctBrandName(implicit sqlContext: SQLContext): (DataFrame, DataFrame) = {
    import sqlContext.implicits._
    //去掉非法字符 词性标点符号w 还有 % '等特殊字符
    sqlContext.udf.register("removeSW", (tx: String) => {
      val ws = tx.wordsNameNature
      val flag = if (ws.size == 1 && ws.map(_._2).contains("w")) 0
      else if (tx.trim.size == 1) 0
      else if (tx.trim.isEmpty) 0
      else 1
      flag
    })
    //生成brandNo
    sqlContext.udf.register("getBrandNo", () => IdGenerator.generateBrandID)
    val brandInfoTb = "TBL_CHMGM_BRAND_INF"
    val brandInfoDF = JdbcUtil.mysqlJdbcDF(brandInfoTb)
    val df = sqlContext.mongoDF("brand")
      .select(regexp_replace(trim($"name"), "[;； ]", "").as("name"))
      .filter("removeSW(name)=1")
    //广播小表
    //互联网品牌中去重银联自有品牌
    val crawlDF = df.as('a)
      .join(broadcast(brandInfoDF.select(trim($"BRAND_NM").as("BRAND_NM"))).as('b),
        $"a.name" !== $"b.BRAND_NM", "leftsemi")
      .selectExpr("getBrandNo()  brand_no", "a.name brand_nm")
    crawlDF.cache()
    (brandInfoDF, crawlDF)
  }
}
