package com.scalatest

import java.io._
import java.nio.file.{Files, Paths}

import com.unionpay.util.JsonUtil
import org.scalatest.FunSuite

import scala.io.Source
import scala.collection.JavaConversions._

/**
  * Created by ywp on 2016/8/9.
  */
class IndustrySuite extends FunSuite {

  test("文件生成一二级分类") {
    val first = Seq("A0", "B0", "C0", "D0", "E0", "F0")
    val path = "C:\\Users\\ywp\\Desktop\\lg.txt"
    val sqlFile = "C:\\Users\\ywp\\Desktop\\lg.sql"
    val pw = new PrintWriter(sqlFile, "UTF-8")
    val sql =
      """
        |INSERT into tbl_content_industry_sub_inf(industry_sub_no,industry_no,industry_sub_nm) VALUES('%s','%s','%s');
      """.stripMargin
    Files.readAllLines(Paths.get(path))
      .toStream
      .filter(!_.isEmpty)
      .map(_.replaceAll("\uFEFF", ""))
      .map(_.split("\t"))
      .filter(x => !first.contains(x(1)))
      .map(s => {
        val Array(name, code) = s
        code.trim match {
          case x: String if x.contains("A") || x.isEmpty => s"${sql.format(code, "A0", name)}"
          case x: String if x.contains("B") => s"${sql.format(code, "B0", name)}"
          case x: String if x.contains("C") => s"${sql.format(code, "C0", name)}"
          case x: String if x.contains("D") => s"${sql.format(code, "D0", name)}"
          case x: String if x.contains("E") => s"${sql.format(code, "E0", name)}"
          case x: String if x.contains("F") => s"${sql.format(code, "F0", name)}"
        }
      })
      .foreach(pw.println)

    pw.close()
  }

  test("逻辑转换") {

    //    val map = Map("A" -> "美食", "B" -> "购物", "C" -> "休闲娱乐", "D" -> "丽人", "E" -> "酒店", "F" -> "生活服务")
    val map = Map("E" -> "酒店")
    val sql =
      """
        |INSERT INTO tbl_industry_etl_logic(type,type_name,origin_name,real_name) VALUES ('%s','%s','%s','%s') ;
      """.stripMargin
    map.foreach(ty => {
      println(ty)
      val (tt, tn) = ty
      val t = s"C:\\Users\\ywp\\Desktop\\trans\\${tt}.txt"
      val s = s"C:\\Users\\ywp\\Desktop\\trans\\${tt}.sql"
      val pw = new PrintWriter(s, "UTF-8")
      val source = Source.fromFile(t)("UTF-8")
      source
        .getLines()
        .toArray
        .filter(!_.isEmpty)
        .map(_.replaceAll("\uFEFF", ""))
        .map(_.split("\t"))
        .map {
          case Array(a, b, c) => if (b.trim.isEmpty) Array(a, c) else Array(a, b, c)
        }
        .filter {
          case Array(t, d, o) => {
            if (t.contains("删除")) false else if (d.contains("删除")) false else if (d.trim.isEmpty) false else true
          }
          //          case ar: Array[String] if ar.size != 3 => println(ar.mkString("[", ",", "]")); false
          case Array(n, o) => if (n.contains("删除") || n.trim.isEmpty) false else true
        }
        .map(a => {
          if (a.size > 2) (a(1).trim, a(2).trim) else (a(0), a(1))
        })
        .groupBy(_._2)
        .mapValues(ar => ar.map(_._1).apply(0))
        .foreach(x => {
          val line = sql.format(s"${tt}0", tn, x._1, x._2)
          //          println(line)
          pw.println(line)
        })
      source.close()
      pw.close()
    })
  }


  test("新的二级分类为空的ObjectId") {
    val path = "D:\\project\\sparketl\\out\\artifacts\\sparketl_jar\\noSub.csv"
    Source.fromFile(path)("GBK")
      .getLines()
      .toStream
      .map(x => x.split(",").head)
      .foreach(println)
  }

  test("无法匹配的店铺") {
    val path1 = "D:\\project\\sparketl\\out\\artifacts\\sparketl_jar\\test_crawl_industry.csv"
    val path2 = "D:\\project\\sparketl\\out\\artifacts\\sparketl_jar\\noSub.csv"
    val lines1 = Source.fromFile(path1)("UTF-8")
      .getLines()
      .toStream
      .map(x => x.split(",").head)
    val lines2 = Source.fromFile(path2)("GBK")
      .getLines()
      .toStream
      .map(x => x.split(",").head)
    val diffIds = lines1.diff(lines2)
    val path = "D:\\project\\sparketl\\out\\artifacts\\sparketl_jar\\diff.csv"
    val pw = new PrintWriter(path, "UTF-8")
    diffIds.foreach(pw.println)
    pw.close()
    //    val num1 = lines1.count(x => true)
    //    println(num1)
    //    val num2 = lines2.count(x => true)
    //    println(num2)
  }

  test("xx") {
    val totalIds = Files.readAllLines(Paths.get("D:\\project\\sparketl\\out\\artifacts\\sparketl_jar\\diff无法匹配数据-原始二级为空.csv"))
      .toStream
      .filter(x => !x.trim.isEmpty)
      .map(_.split(",").head).drop(1)

    val shopEmptyIds = Source.fromFile("D:\\project\\sparketl\\out\\artifacts\\sparketl_jar\\店铺名为空（按现有逻辑）.csv")("GBK")
      .getLines()
      .toStream
      .filter(x => !x.trim.isEmpty)
      .map(_.split(",").head)
      .drop(1)

    val newSubEmptyIds =
      Source.fromFile("D:\\project\\sparketl\\out\\artifacts\\sparketl_jar\\new_empty_sub_ids.csv")("GBK")
        .getLines()
        .toStream
        .filter(x => !x.trim.isEmpty)
        .map(_.split(",").head)
        .drop(1)
    val re = "D:\\project\\sparketl\\out\\artifacts\\sparketl_jar\\remain_ids.csv"
    val pw = new PrintWriter(re, "utf-8")
    val remainIds = totalIds.diff(shopEmptyIds).diff(newSubEmptyIds)
    remainIds.foreach(pw.println)
    pw.close()
  }

  test("二级分类为空扣除店铺名为空的") {
    val subEmptyIds = Source.fromFile("D:\\project\\sparketl\\out\\artifacts\\sparketl_jar\\新二级分类空（按现有逻辑）.csv")("UTF-8")
      .getLines()
      .toStream
      .filter(x => !x.trim.isEmpty)
      .map(_.split(",").head.trim)
      .drop(1)
    val shopEmptyIds = Source.fromFile("D:\\project\\sparketl\\out\\artifacts\\sparketl_jar\\店铺名为空（按现有逻辑）.csv")("UTF-8")
      .getLines()
      .toStream
      .filter(x => !x.trim.isEmpty)
      .map(_.split(",").head.trim)
      .drop(1)
    val dd = subEmptyIds.diff(shopEmptyIds)
    val re = "D:\\project\\sparketl\\out\\artifacts\\sparketl_jar\\新二级分类空（按现有逻辑）扣除店铺为空.csv"
    val pw = new PrintWriter(re, "utf-8")
    Source.fromFile("D:\\project\\sparketl\\out\\artifacts\\sparketl_jar\\新二级分类空（按现有逻辑）.csv")("UTF-8")
      .getLines()
      .toStream.filter(x => dd.contains(x.split(",").head.trim))
      .foreach(pw.println)
    pw.close()


  }

  test("xxxxxx") {
    val newSubEmptyIds = Source.fromFile("D:\\project\\sparketl\\out\\artifacts\\sparketl_jar\\新二级分类空按现有逻辑.csv")("UTF-8")
      .getLines()
      .toStream
      .filter(x => !x.trim.isEmpty)
      .map(_.split(",").head.trim)
      .drop(1)

    val oldSubEmptyIds = Source.fromFile("D:\\project\\sparketl\\out\\artifacts\\sparketl_jar\\原始二级分类为空.csv")("UTF-8")
      .getLines()
      .toStream
      .filter(x => !x.trim.isEmpty)
      .map(_.split(",").head.trim)
      .drop(1)

    println(newSubEmptyIds.count(x => true))
    println(oldSubEmptyIds.count(x => true))
    val dd = oldSubEmptyIds.diff(newSubEmptyIds)
    println(dd.count(x => true))

  }

  test("遗漏去重") {
    val remainNames = Source.fromFile("D:\\project\\sparketl\\out\\artifacts\\sparketl_jar\\缺漏.csv")("GBK")
      .getLines()
      .toStream
      .filter(x => !x.trim.isEmpty)
      .map(x => {
        val sp = x.split(",")
        (sp(2).trim.replaceAll("\\[", "").replaceAll("\\]", ""), sp(3).trim.replaceAll("\\[", "").replaceAll("\\]", ""))
      })
      .drop(1)

    val d = remainNames
      .groupBy(_._2)
      .mapValues(x => {
        x.map(_._1).toSet
      })

    val re = "D:\\project\\sparketl\\out\\artifacts\\sparketl_jar\\未整理遗漏.csv"
    val pw = new PrintWriter(re, "utf-8")
    d.foreach {
      case (k, v) => {
        pw.println(s"${k},${v.mkString("[", ",", "]")}")
      }
    }

    pw.close()
    //    val oldSubNames = Source.fromFile("D:\\project\\sparketl\\out\\artifacts\\sparketl_jar\\old_sub.csv")("GBK")
    //      .getLines()
    //      .toStream
    //      .map(_.trim)
    //      .filter(x => !x.isEmpty)
    //      .drop(1)
    //      .distinct
    //
    //    val remainSubNames = remainNames.map(_._2).distinct
    //
    //    println(remainSubNames.count(x => true))
    //    println(oldSubNames.count(x => true))
    //
    //    val dd = remainSubNames.diff(oldSubNames)
    //    dd.foreach(println)

  }

}
