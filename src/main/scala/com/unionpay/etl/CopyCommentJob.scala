package com.unionpay.etl

import com.unionpay.db.jdbc.JdbcUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Created by ywp on 2016/7/8.
  */
object CopyCommentJob {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("CopyCommentJob--评论信息任务")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    etl
    sc.stop()
  }

  def etl(implicit sqlContext: SQLContext) = {
    val copyTable = "tbl_content_brand_comment_inf"
    val commentTB = "TBL_CHMGM_BRAND_COMMENT_INF"
    val commentDF = JdbcUtil.mysqlJdbcDF(commentTB).dealTimeStamp
    JdbcUtil.save2Mysql(copyTable)(commentDF)
  }

}

