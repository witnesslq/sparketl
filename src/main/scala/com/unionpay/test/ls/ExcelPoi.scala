package com.unionpay.test.ls

import java.io.File

import org.apache.poi.ss.usermodel.Row
import org.apache.poi.xssf.usermodel.XSSFWorkbook

/**
  * Created by ls on 2016/10/13.
  */
object ExcelPoi {

  def main(args: Array[String]) {

    val path = "C:\\Users\\ls\\Desktop\\excel\\test.xlsx"

    val excel = new XSSFWorkbook(new File(path))
    val sheet = excel.getSheetAt(0)

    val minRow = 0
    val maxRow = sheet.getLastRowNum
    for (i <- minRow until maxRow) {
      println("Row#" + i + "=>")
      val row = sheet.getRow(i)
      val minCell = 0
      val maxCell = row.getLastCellNum
      for (j <- minCell until maxCell) {
        println("Cell#" + j + "=>")
        val cell = row.getCell(j, Row.CREATE_NULL_AS_BLANK)
        println(cell.toString)
      }
    }
  }
}
