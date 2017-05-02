package com.unionpay.test.ls

import java.io.PrintWriter

import com.unionpay.db.jdbc.JdbcUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/10/28.
  */
object DistinctData {

  def main(args: Array[String]) {


    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("getMongoShopData == 抽取银行对应门店信息")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2000m")
      .set("spark.yarn.driver.memoryOverhead", "2048")
      .set("spark.yarn.executor.memoryOverhead", "2000")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    sqlContext.setConf("spark.sql.shuffle.partitions", "32")

    val parShopDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\mongo_bank_shop")
      .filter($"_id.oid" === "57a05bed951f59b1cfc3d901")
      .selectExpr("description")
      .map(_.getAs[String]("description"))
      .collect()

    parShopDF.foreach(r => println(r.size + "\n\n" + r))
    //      .foreach(println)

    //    parShopDF.show()

    //    val shopDF = JdbcUtil.mysqlJdbcDF("tbl_content_shop_inf_20161201", "sink")
    //      .selectExpr("src_shop_no")
    //
    //    val df1 = parShopDF.except(shopDF)
    //    val df2 = shopDF.except(parShopDF)
    //
    //    val pw = new PrintWriter("/tmp/ls/diff.txt")
    //
    //    println(df1.count())
    //    println(df2.count())
    //    pw.println("====================df1======================")
    //    df1.map(_.getAs[String]("src_shop_no")).collect().foreach(pw.println)
    //    pw.println("====================df2======================")
    //    df2.map(_.getAs[String]("src_shop_no")).collect().foreach(pw.println)
    //    pw.close()

    sc.stop()

  }


}
