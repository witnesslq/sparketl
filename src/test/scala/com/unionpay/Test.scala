package com.unionpay

import java.io.PrintWriter
import java.util.UUID

import com.sparktest.CsvBaseTest
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

import scala.io.Source

/**
  * Created by ls on 2016/11/29.
  */
class Test extends FunSuite with CsvBaseTest {
  test("UUID") {

    val pw = new PrintWriter("C:\\Users\\ls\\Desktop\\20161202\\ids.txt")
    var i = 0
    while (i < 10000000) {
      pw.println(UUID.randomUUID().toString.replaceAll("-", ""))
      i = i + 1
    }
    pw.close()
  }
}
