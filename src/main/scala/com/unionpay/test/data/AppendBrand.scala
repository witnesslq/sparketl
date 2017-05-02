package com.unionpay.test.data

import com.unionpay.db.jdbc.JdbcUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.XJdbcUtil._
import com.unionpay.db.jdbc.JdbcSaveMode

/**
  * Created by ls on 2016/10/19.
  */
object AppendBrand {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("GetBrandName === 获取品牌信息")
      //      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "32")

    val appendDF = JdbcUtil.mysqlJdbcDF("tbl_chmgm_brand_tmp", "sink")

    JdbcUtil.saveToMysql("tbl_chmgm_brand_inf", JdbcSaveMode.Append, rootNode = "source")(appendDF)

    sc.stop()

  }

}
