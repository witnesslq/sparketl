package com.unionpay.test.ls

import java.io.PrintWriter

import com.unionpay.db.jdbc.JdbcUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by ls on 2016/9/9.
  */
object TestContains {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("TestContains")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")

    val path = "C:\\Project\\sbt\\scalademo\\data\\del\\tbl_chmgm_ticket_bill_bas_inf.del"

    val pw = new PrintWriter("C:\\Users\\ls\\Desktop\\category\\diff")
    val df = JdbcUtil.mysqlJdbcDF("TBL_CHMGM_TICKET_BILL_BAS_INF")
      .selectExpr("trim(BILL_ID) BILL_ID")
      .map(_.getAs[String]("BILL_ID"))
      .collect()


    val files = Source.fromFile(path, "UTF-8")
      .getLines()
      .toArray
      .map(r => {
        val x = r.split("#")
        x.apply(0).replaceAll("[\"|-]", "").trim
      })


    val res = files.diff(df)
    println(res.length)

    res.foreach(pw.println(_))
    pw.close()

    sc.stop()
  }

}
