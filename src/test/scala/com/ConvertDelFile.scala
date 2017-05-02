package com

import java.io.PrintWriter
import java.nio.file.{Files, Path, Paths}
import java.util.stream.Collectors

import scala.collection.JavaConversions._
import scala.io.Source

/**
  * Created by ywp on 2016/7/23.
  */
object ConvertDelFile {

  private val fileNames = Seq(
    "TBL_CHMGM_BRAND_INF",
    "TBL_CHMGM_BRAND_PIC_INF",
    "TBL_CHMGM_BUSS_DIST_INF",
    "TBL_CHMGM_TICKET_COUPON_INF",
    "TBL_CHMGM_MCHNT_PARA",
    "TBL_CHMGM_CHARA_GRP_DEF_FLAT",
    "TBL_CHMGM_PREFERENTIAL_MCHNT_INF",
    "TBL_CHMGM_BRAND_COMMENT_INF",
    "TBL_CHMGM_BUSS_ADMIN_DIVISION_CD"
    , "t"
  )

  //  private val FilePath = "D:\\银联\\mongo数据\\0722导出表"
  private val FilePath = "C:\\Users\\ywp\\Desktop"

  def main(args: Array[String]) {
    tarnsFormDel2Sql

  }

  //todo 由于一些特殊换行符导致数据不是一个文本 改用spark来读取文件
  def test = {
    val lines = Source.fromFile("C:\\Users\\ywp\\Desktop\\TBL_CHMGM_BRAND_INF.20160722 - 副本.csv")("GBK")
      .getLines().toArray
    lines.foreach(line => {
      println(s"insert into  values (${line});")
    })
  }

  def tarnsFormDel2Sql: Unit = {
    val sqlFiles = Files.walk(Paths.get(FilePath))
      .collect(Collectors.toList[Path])
      .map(_.getFileName.toString)
      //      .filter(_.endsWith("del"))
      .filter(_.endsWith("csv"))
      .map(n => s"$FilePath\\${n}")
      .toArray

    sqlFiles.foreach(s => {
      val head2 = Source.fromFile(s)("UTF-8").getLines().toArray
      val table = fileNames.find(l => s.toLowerCase.contains(l.toLowerCase()))
      //      val pw = new PrintWriter(s"${FilePath}\\${table.get}.sql")
      head2.foreach(line => {
        table match {
          case None => println("no data")
          case Some(st) => {
            //            pw.println(s"insert into $st values (${line});")
            if (line.contains("hello")) {
              println(s"insert into $st values (${line});")
            }

          }
        }
      })
      //      pw.close()
    })
  }
}

