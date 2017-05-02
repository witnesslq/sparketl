package com.unionpay.test.Excel

import java.io.{File, PrintWriter}

import org.apache.poi.ss.usermodel.Row
import org.apache.poi.xssf.usermodel.{XSSFCell, XSSFWorkbook}

import scala.collection.mutable.ArrayBuffer


/**
  * Created by ls on 2016/10/12.
  */
object ShopExcel2Csv {

  val str =
    "MCHNT_CD," + "MCHNT_NM," + "SHOP_VALID_ST," +
      "NEW_NAME," + "MCHNT_ADDR," + "MCHNT_PHONE," +
      "NEW_PHONE," + "MCHNT_URL," + "MCHNT_CITY_CD," +
      "MCHNT_COUNTY_CD," + "MCHNT_PROV," + "BUSS_DIST_CD," +
      "MCHNT_TYPE_ID," + "COOKING_STYLE_ID," + "REBATE_RATE," +
      "DISCOUNT_RATE," + "AVG_CONSUME," + "POINT_MCHNT_IN," +
      "DISCOUNT_MCHNT_IN," + "PREFERENTIAL_MCHNT_IN," +
      "OPT_SORT_SEQ," + "KEYWORDS," + "MCHNT_DESC," + "REC_CRT_TS," +
      "REC_UPD_TS," + "ENCR_LOC_INF," + "COMMENT_NUM," + "FAVOR_NUM," +
      "SHARE_NUM," + "BUSS_HOUR," + "NEW_BUSS_HOUR," + "TRAFFIC_INF," +
      "FAMOUS_SERVICE," + "COMMENT_VALUE," + "CONTENT_ID," + "MCHNT_ST," +
      "MCHNT_FIRST_PARA," + "MCHNT_SECOND_PARA," + "MCHNT_LONGITUDE," +
      "MCHNT_LATITUDE," + "MCHNT_LONGITUDE_WEB," + "MCHNT_LATITUDE_WEB," +
      "CUP_BRANCH_INS_ID_CD," + "BRANCH_NM," + "BRAND_ID," + "BUSS_BMP," +
      "TERM_DIFF_STORE_TP_IN," + "REC_ID," + "AMAP_LONGITUDE," + "AMAP_LATITUDE," +
      "flashPay," + "free," + "wifi," + "park," + "card," + "brandName," + "NEW_Brand," +
      "simlarity," + "sim2"

  def main(args: Array[String]) {

    val dir = new File("C:\\Users\\ls\\Desktop\\csv_data")
    val children = dir.listFiles.filter(_.isFile)

    children.foreach(f => {
      val name = f.getName.replaceAll(".xlsx", "")

      println(f.getAbsolutePath + "========>" + name)
      val pw = new PrintWriter(s"C:\\Users\\ls\\Desktop\\test\\${name}.csv")
      val excel = new XSSFWorkbook(f)
      val sheet = excel.getSheetAt(0)
      pw.println(str)
      val minRow = 1
      val maxRow = sheet.getLastRowNum

      for (i <- minRow to maxRow) {
        val array = new ArrayBuffer[XSSFCell]
        val row = sheet.getRow(i)
        val minCell = 0
        val maxCell = 58

        for (j <- minCell to maxCell) {
          val cell = row.getCell(j, Row.CREATE_NULL_AS_BLANK)
          array += cell
        }
        val res = array.mkString("\"", "\",\"", "\"")
        pw.println(res)
      }
      pw.close()
    })
  }
}




