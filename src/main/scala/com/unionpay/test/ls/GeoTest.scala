package com.unionpay.test.ls

import java.io.PrintWriter
import java.math.BigDecimal
import java.util

import com.mongodb.{BasicDBList, BasicDBObject}
import com.unionpay.db.jdbc.JdbcUtil
import com.unionpay.db.mongo.MongoUtil._
import org.apache.spark.sql.{DataFrame, SQLContext}
import java.math.{BigDecimal => javaBigDecimal}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Created by ls on 2016/9/12.
  */
object GeoTest {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("shopInfoJob--店铺信息任务")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2000m")
      .set("spark.yarn.driver.memoryOverhead", "2048")
      .set("spark.yarn.executor.memoryOverhead", "2000")
      .set("spark.network.timeout", "300s")
      //todo 云主机 经常网络超时
      .set("spark.executor.heartbeatInterval", "30s")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")
    import sqlContext.implicits._


    val brandDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\数据\\上线后数据备份\\上线数据\\处理前数据\\exportData\\data1202\\tbl_content_brand_inf30")
      .filter("length(src_brand_no) > 10")
      .selectExpr("brand_no")
      .map(_.getAs[String]("brand_no"))
      .collect()

    println(brandDF.length)

    val shopDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\数据\\上线后数据备份\\上线数据\\处理前数据\\exportData\\data1202\\tbl_content_shop_inf30")
      .filter($"shop_valid_st" === 1)
      .selectExpr("src_shop_no", "brand_no")
      .filter($"brand_no".isin(brandDF: _*))

    println(shopDF.count())

    /*val shopDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\20161202\\simi_result")

    val shopIds = shopDF.selectExpr("explode(b_list) src_shop_no")
      .map(_.getAs[String]("src_shop_no"))
      .collect()

      println(shopIds.length)
      println(shopIds.distinct.length)

    val df = JdbcUtil.mysqlJdbcDF("tbl_content_shop_inf", "sink")
      .selectExpr("src_shop_no", "shop_valid_st")
      .filter($"src_shop_no".isin(shopIds: _*))
      .filter($"shop_valid_st" === "1")*/


    /*val shopIds = shopDF.selectExpr("src_shop_no")
      .map(_.getAs[String]("src_shop_no"))
      .collect()

    val sIds = JdbcUtil.mysqlJdbcDF("tbl_content_shop_inf", "sink")
      .selectExpr("src_shop_no", "shop_valid_st")
      .filter($"src_shop_no".isin(shopIds: _*))
      .filter($"shop_valid_st" === "0")
      .selectExpr("src_shop_no")
      .map(_.getAs[String]("src_shop_no"))
      .collect()

    val update = shopDF.filter(!$"src_shop_no".isin(sIds: _*))
      .selectExpr("src_shop_no relation_src_shop_no", "explode(b_list) src_shop_no")
      .map(r => {
        val rssn = r.getAs[String]("relation_src_shop_no")
        val ssn = r.getAs[String]("src_shop_no")
        s"update tbl_content_shop_inf set relation_src_shop_no='$rssn' where src_shop_no='$ssn';"
      }).collect()

    def writeSql(name: String, ds: Seq[String]) = {
      val pw = new PrintWriter(s"C:\\Users\\ls\\Desktop\\20161202\\$name.sql")
      ds.foreach(pw.println)
      pw.close()
    }
    writeSql("relation_update", update)
*/

    sc.stop()
  }
}
