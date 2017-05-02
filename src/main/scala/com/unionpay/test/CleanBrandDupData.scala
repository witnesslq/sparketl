package com.unionpay.test

import com.unionpay.db.jdbc.JdbcUtil._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by ywp on 2016/8/21.
  */
object CleanBrandDupData {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("CleanBrandDupData--品牌jdbc去重")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    dealDup

    sc.stop()

  }


  def dealDup: Unit = {
    val connection = getMysqlJdbcConnection()
    val statement = connection.createStatement
    try {
      val rs = statement.executeQuery("select brand_nm,count(1) total from tbl_content_brand_inf group by brand_nm having total >1")
      val dupBrand = ArrayBuffer[String]()
      while (rs.next) {
        val brandName = rs.getString("brand_nm")
        dupBrand.append(brandName)
      }
      dupBrand.foreach(bn => {
        val sql1 = s"""select row_id,industry_no from tbl_content_brand_inf where brand_nm="$bn""""
        val rest = statement.executeQuery(sql1)
        val ids = ArrayBuffer.empty[Int]
        while (rest.next) {
          val rowId = rest.getInt("row_id")
          val indNo = rest.getString("industry_no")
          if (indNo.trim.isEmpty) ids.append(rowId)
        }
        //random remain one row from dup rows
        val deleteIds = ids.drop(1)
        val sql = s"delete from tbl_content_brand_inf where row_id in (${deleteIds.mkString(",")})"
        if (deleteIds.size > 1) println(sql)
        //        statement.execute(sql)
      })

    } catch {
      case e: Exception => throw e
    } finally {
      statement.close()
      connection.close()
    }
  }
}
