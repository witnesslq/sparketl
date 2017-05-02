package com.unionpay.test.Excel

import java.io.{File, PrintWriter}

import org.apache.poi.ss.usermodel.Row
import org.apache.poi.xssf.usermodel.{XSSFCell, XSSFWorkbook}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by ls on 2016/10/14.
  */
object BrandExcel2Csv {
  val str = "brandId," + "brandName," + "new_brandName"

  def main(args: Array[String]) {

    val dir = new File("C:\\Users\\ls\\Desktop\\11")
    val children = dir.listFiles.filter(_.isFile)

    //    val fw = new FileWriter("C:\\Users\\ls\\Desktop\\brand.csv", true)
    //    val pw = new PrintWriter(fw)

    //    pw.println(str)
    children.foreach(f => {
      val name = f.getName.replaceAll(".xlsx", "")

      println(f.getAbsolutePath + "========>" + name)
      val pw = new PrintWriter(s"C:\\Users\\ls\\Desktop\\test\\${name}.csv")

      val excel = new XSSFWorkbook(f)
      val sheet = excel.getSheetAt(0)

      val minRow = 1
      val maxRow = sheet.getLastRowNum

      for (i <- minRow to maxRow) {
        val array = new ArrayBuffer[XSSFCell]
        val row = sheet.getRow(i)

        val brandId = row.getCell(44, Row.CREATE_NULL_AS_BLANK)
        val brandName = row.getCell(55, Row.CREATE_NULL_AS_BLANK)
        val new_Name = row.getCell(56, Row.CREATE_NULL_AS_BLANK)

        array += brandId
        array += brandName
        array += new_Name

        val res = array.mkString("\"", "\",\"", "\"")
        pw.println(res)
      }
      pw.close()
    })
  }

}
