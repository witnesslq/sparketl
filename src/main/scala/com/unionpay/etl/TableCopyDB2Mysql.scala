package com.unionpay.etl

import com.unionpay.db.jdbc.{JdbcUtil, MysqlConnection}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ywp on 2016/7/21.
  */
object TableCopyDB2Mysql {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("TableCopyDB2Mysql--db2拷贝数据到mysql")
      //      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array())
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    val tables = Seq(
      "TBL_CHMGM_BRAND_INF",
      "TBL_CHMGM_BRAND_PIC_INF",
      "TBL_CHMGM_BUSS_DIST_INF",
      "TBL_CHMGM_TICKET_COUPON_INF",
      "TBL_CHMGM_MCHNT_PARA",
      "TBL_CHMGM_CHARA_GRP_DEF_FLAT",
      "TBL_CHMGM_PREFERENTIAL_MCHNT_INF",
      "TBL_CHMGM_BRAND_COMMENT_INF",
      "TBL_CHMGM_BUSS_ADMIN_DIVISION_CD"
    )
    val mysql = MysqlConnection.build("db2mysql", "sink")
    import mysql._
    tables.foreach(table => {
      val db2DF = JdbcUtil.db2JdbcDF(table).dealTimeStamp
      db2DF.save2DB(table)
    })

    sc.stop()

  }

}
