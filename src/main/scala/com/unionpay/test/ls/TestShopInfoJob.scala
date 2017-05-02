package com.unionpay.test.ls

import com.unionpay.util.IdGenerator
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import com.unionpay.db.jdbc.JdbcUtil._
import com.unionpay.db.jdbc.JdbcSaveMode._

/**
  * Created by ls on 2016/10/9.
  */
object TestShopInfoJob {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("shopInfoJob--店铺信息任务")
      .setMaster("local[*]")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")
    val sb = udf(() => IdGenerator.generateShopID)

    val df = Seq(("sb11", "1"), ("sb21", "2")).toDF("name", "src_shop_no")
    df.selectExpr("name", "src_shop_no")
    val shopDF = mysqlJdbcDF("sb", "sink").selectExpr("src_shop_no")

    val etlDF = df.selectExpr("src_shop_no")

    val updateDF = etlDF.intersect(shopDF)
    updateDF.show()

    val insertDF = etlDF.except(shopDF)
    insertDF.show()

    val updateShopDF = df.join(updateDF, df("src_shop_no") === updateDF("src_shop_no"), "leftsemi")
    val insertShopDF = df.join(insertDF, df("src_shop_no") === insertDF("src_shop_no"), "leftsemi")
      .withColumn("shop_no", sb())
    insertShopDF.printSchema()
    insertShopDF.show()

    saveToMysql("sb", Upsert)(updateShopDF)
    saveToMysql("sb", Upsert)(insertShopDF)

    sc.stop()
  }

}
