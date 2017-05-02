package com.unionpay.test.Excel

import java.io.{File, PrintWriter}

import org.apache.poi.ss.usermodel.Row
import org.apache.poi.xssf.usermodel.{XSSFCell, XSSFWorkbook}

import scala.collection.mutable.ArrayBuffer


/**
  * Created by ls on 2016/10/12.
  */
object BankExcel2Csv {


  def main(args: Array[String]) {
    val str =
      "bankCode," + "shopId," + "category," +
        "subCategory," + "country," + "city," +
        "brand," + "new_brand," + "shopName," +
        "address," + "description," + "score," +
        "logo," + "shop_lnt," + "shop_lat," +
        "telephones," + "new_telephones," + "tradingArea," +
        "pictureList," + "tags," +
        "averagePrice," + "hours," + "status," + "type," +
        "createAt," + "updateAt," + "landId"

    val dir = new File("C:\\Users\\ls\\Desktop\\11")
    val children = dir.listFiles.filter(_.isFile)

    children.foreach(f => {
      val name = f.getName.replaceAll(".xlsx", "")

      println(f.getAbsolutePath + "========>" + name)
      val pw = new PrintWriter(s"C:\\Users\\ls\\Desktop\\11\\${name}.csv")
      pw.println(str)
      val excel = new XSSFWorkbook(f)
      val sheet = excel.getSheetAt(0)
      val minRow = 1
      val maxRow = sheet.getLastRowNum

      for (i <- minRow to maxRow) {
        val array = new ArrayBuffer[XSSFCell]
        val row = sheet.getRow(i)
        val minCell = 0
        val maxCell = 26

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




