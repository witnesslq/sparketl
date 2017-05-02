package com.unionpay.test.online

import java.io.PrintWriter

import com.unionpay.db.jdbc.{JdbcSaveMode, JdbcUtil}
import com.unionpay.test.data.DealArea._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/11/30.
  */
object ShopDistinc {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("InsertData to CMS === 插入数据")
      //      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "32")

    val shopDF = JdbcUtil.mysqlJdbcDF("tbl_content_shop_inf_20161129", "sink")

    val shopIdsDF = shopDF.selectExpr("src_shop_no")

    println("shopDF:" + shopDF.count())


    val existedShopDF = sqlContext.read.parquet("/tmp/ls/tbl_content_shop_inf29")
      .selectExpr("src_shop_no")

    val sameIds = shopIdsDF.intersect(existedShopDF).map(_.getAs[String]("src_shop_no")).collect()
    println("sameIds" + sameIds.size)

    val tmpDF = shopDF.filter(!($"src_shop_no".isin(sameIds: _*)))

    val pw = new PrintWriter("/tmp/ls/del.sql")

    val delIds = tmpDF.selectExpr("src_shop_no").distinct().map(r => {
      val so = r.getAs[String]("src_shop_no")
      s"'$so'"
    }).collect()

    pw.println(s"delete from tbl_content_shop_inf where src_shop_no in ${delIds.mkString("(", ",", ")")}")
    pw.close()


    JdbcUtil.saveToMysql("tbl_content_shop_inf_20161129_bak", JdbcSaveMode.Upsert)(tmpDF)

    println("tmpDF:" + tmpDF.count())

    sc.stop()
  }

}
