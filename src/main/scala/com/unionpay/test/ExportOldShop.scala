package com.unionpay.test

import java.io.PrintWriter

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import com.unionpay.db.mongo.MongoUtil._
import org.apache.spark.sql.hive.HiveContext
import org.graphframes.GraphFrame
import com.unionpay.db.jdbc.JdbcUtil._

/**
  * Created by ywp on 2016/8/10.
  */
object ExportOldShop {

  private lazy val rex1 = """[0-9]+[元减 减 立减 再减].?""".r
  private lazy val rex2 ="""[0-9]+折""".r
  private lazy val rex3 ="""^-?[1-9]\d*$""".r

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("ExportOldShop")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")
    import sqlContext.implicits._

    //    val df = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\similarity_test")
    //    val df2 = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\similarity")

    //    println("df:"+df.count())
    //    println("df2:"+df2.count())

    /*val seq = Seq("专享", "银联", "券", "观影", "银行", "邮储", "IC卡", "信用卡", "优惠", "6+2", "权益", "活动", "测试", "扫码", "云闪付", "积分", "信用卡", "重阳", "62", "六二", "悦享", "测试", "一元风暴", "约惠星期六")
    val illegalBrandName = udf((brandName: String) => {
      !seq.filter(brandName.contains).isEmpty || rex1.findFirstIn(brandName).isDefined || rex2.findFirstIn(brandName).isDefined || rex3.findFirstIn(brandName).isDefined
    })

    val shopDF = mysqlJdbcDF("tbl_chmgm_preferential_mchnt_inf")
      .selectExpr("BRAND_ID", "MCHNT_NM", "MCHNT_CD")
    val brandDF = mysqlJdbcDF("tbl_chmgm_brand_inf").selectExpr("BRAND_ID", "BRAND_NM")
    val activityDF = shopDF.as('a)
      .join(brandDF.as('b), $"a.BRAND_ID" === $"b.BRAND_ID")
      .selectExpr("a.MCHNT_CD", "b.BRAND_NM")
      .filter(illegalBrandName($"BRAND_NM"))

    val pw = new PrintWriter("/tmp/activity.csv", "UTF-8")
    pw.println("店铺Id,品牌")
    activityDF.map(_.mkString(",")).collect().foreach(pw.println)
    pw.close()*/

    val simDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\similarity")
      .selectExpr("shopNo", "otherNo")
    println(simDF.count())
    val v = simDF.selectExpr("shopNo id").unionAll(simDF.selectExpr("otherNo id")).distinct().orderBy($"id")
    val e = simDF.selectExpr("shopNo src", "otherNo dst").withColumn("relationship", lit("sim"))
    val g = GraphFrame(v, e)
    val result = g.connectedComponents.run()
    println(result.count())

    /*val pw2 = new PrintWriter("/tmp/dupshop.csv", "UTF-8")
    result.groupBy("component")
      .agg(collect_set("id").as("dirtyIds"))
      .filter("size(dirtyIds)>1")
      .map(_.getAs[Seq[String]]("dirtyIds").mkString("[", "|", "]"))'
      .collect()
      .foreach(pw2.println)
    pw2.close()*/

    val total = result.groupBy("component")
      .agg(collect_set("id").as("dirtyIds"))


    val tmpDF = result.as('a)
      .join(total.as('b),$"a.component" === $"b.component")
      .selectExpr("a.id","b.dirtyIds")

    tmpDF.printSchema()
    tmpDF.show(5)
    println(tmpDF.count())

  }

}
