package com.scalatest

import java.io.File

import org.scalatest.FunSuite
import org.apache.poi.ss.usermodel.{Cell, Row, Sheet, Workbook}
import org.apache.poi.xssf.usermodel._

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by ywp on 2016/8/19.
  */
class Excel extends FunSuite {

  test("一二级分类") {
    val delet ="""[删除 无效 分类]""".r
    val path = "C:\\Users\\ywp\\Desktop\\一、二级分类整合结果0811.xlsx"
    val excel = new XSSFWorkbook(new File(path))
    val industry = ArrayBuffer[(String, String)](("一级分类code", "一级分类名称"))
    val subIndustry = ArrayBuffer[(String, String, String)](("二级分类code", "一级分类code", "二级分类名称"))
    val logic = ArrayBuffer[(String, String, String, String)]()
    for (i <- 2.until(excel.getNumberOfSheets)) {
      val sheet = excel.getSheetAt(i)
      val needRows = sheet.rowIterator().toSeq.drop(1)
      val codeCategory = needRows.map(r => {
        val it = r.cellIterator().toSeq.map(_.getStringCellValue).zipWithIndex.map(_.swap)
        val ll = if (it.size < 3) (0, "") else it.dropRight(1).last
        val code = if (it.size < 5) (0, "") else if (ll._2.matches("[A-Z].*")) ll else (0, "")
        val res = code._1 match {
          case x: Int if x == 0 => ("", "")
          case index: Int => {
            val category = it.toSeq(index - 1)._2
            (code._2.trim, category.trim)
          }
        }
        res

      }).filterNot(x => x._1.isEmpty || x._2.isEmpty).partition(_._1.contains("0"))
      val ind = codeCategory._1
      industry.appendAll(codeCategory._1)
      val sub = codeCategory._2.map(t => {
        (t._1, ind.head._1, t._2)
      })
      subIndustry.appendAll(sub)
      val (tp, type_name) = ind.head
      val log = needRows
        .filterNot(r => {
          val x = r.cellIterator().mkString("")
          x.contains("删除") || x.contains("无效") || x.contains("分类名代码")
        })
        .map(r => r.cellIterator().map(_.getStringCellValue).toSeq)
        .map(cells => {
          val rx =
            """[重复 疑问 要不要]""".r
          val originName = if (cells.size <= 2) "" else if (rx.findFirstIn(cells.headOption.getOrElse("")).isDefined) cells.drop(1).tail.headOption getOrElse "" else (cells.tail.headOption getOrElse "")
          val realName = if (cells.size <= 2) "" else if (rx.findFirstIn(cells.headOption.getOrElse("")).isDefined) cells.drop(1).headOption getOrElse "" else (cells.headOption getOrElse "")
          (originName.trim, realName.trim)
        }).filterNot(x => x._1.isEmpty || x._2.isEmpty).distinct.map(x => (tp.trim, type_name.trim, x._1, x._2))
      logic.appendAll(log)
    }

    //    industry.foreach(println)
    //    subIndustry.foreach(println)
    val sql = "insert into tbl_industry_etl_logic(type,type_name,origin_name,real_name) values ('%s','%s','%s','%s') ;"
    logic.map(x => Seq(x._1, x._2, x._3, x._4))
      .filterNot(_.mkString("").containsSlice(Seq("备注", "删除", "无效", "要不要")))
      .foreach(x => println(sql.format(x: _*)))
    excel.close()
  }


  test("一二级分类") {

    val t1 = "D:\\project\\sparketl\\target\\scala-2.10\\sim_crawl_shop.csv"
    val t2 = "D:\\project\\sparketl\\target\\scala-2.10\\sim.csv"
    Source.fromFile(t1)("UTF-8")
      .getLines()
      .toStream

  }

}