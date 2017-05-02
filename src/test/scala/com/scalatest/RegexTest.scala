package com.scalatest

import org.scalatest.FunSuite

import scala.io.Source

/**
  * Created by ywp on 2016/8/30.
  */
class RegexTest extends FunSuite {

  test("正则提取数字") {
    val text = "10:00～22:00，||10:00～22:30"
    val rx ="""\d{1,2}:\d{2}""".r
    rx.findAllIn(text).toStream
  }

  test("包含中文") {
    val texts = Seq("小明你好1", "aaa11", "1111")
    val ch ="""[\u4e00-\u9fa5]""".r
    println(ch.toString())
    texts.foreach(x => {
      println(x, ch.findFirstIn(x).isDefined)
    })
  }

  test("最后一个数字去掉") {
    val lastNumRex = """(.+?)(\d+)$"""
    val text = "肯德基广州番禺富丽KFC（GZH251）"
    val tmp = text.replaceAll(lastNumRex, "$1")

  }

  test("ree") {
    val text = "（）() xx"
    val x = text.replaceAll("[（）() ]", "")
    println(x)
  }

}
