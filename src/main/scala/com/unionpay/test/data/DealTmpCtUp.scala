package com.unionpay.test.data

import com.unionpay.db.jdbc.{JdbcSaveMode, JdbcUtil}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/11/3.
  */
object DealTmpCtUp {


  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("CalculateData---数据统计")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.yarn.driver.memoryOverhead", "1024")
      .set("spark.yarn.executor.memoryOverhead", "2000")
      .set("spark.network.timeout", "300s")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "32")
    import sqlContext.implicits._

    val db2ShopDF = JdbcUtil.mysqlJdbcDF("tbl_chmgm_preferential_mchnt_inf_sb")
      .selectExpr("MCHNT_CD", "REC_CRT_TS", "REC_UPD_TS")

    println("db2ShopDF::" + db2ShopDF.count())

    val tmpShopDF = JdbcUtil.mysqlJdbcDF("tbl_chmgm_shop_tmp", "sink")

    println("tmpShopDF::" + tmpShopDF.count())

    val df = tmpShopDF.as('a).drop("REC_CRT_TS").drop("REC_UPD_TS")
      .join(db2ShopDF.as('b), $"a.MCHNT_CD" === $"b.MCHNT_CD", "left_outer")
      .selectExpr("a.*", "coalesce(b.REC_UPD_TS,'0000-00-00 00:00:00') REC_UPD_TS", "coalesce(b.REC_CRT_TS,'0000-00-00 00:00:00') REC_CRT_TS")

    println("df::" + df.count())

    JdbcUtil.save2Mysql("tbl_chmgm_shop_tmp_20161103")(df)
    sc.stop()
  }
}
