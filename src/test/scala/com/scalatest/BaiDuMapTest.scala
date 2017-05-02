package com.scalatest

import org.scalatest.FunSuite

import scala.io.Source

/**
  * Created by ywp on 2016/8/18.
  */
class BaiDuMapTest extends FunSuite {

  test("rest") {
    val url = "http://api.map.baidu.com/geocoder/v2/?ak=zkQ0nNnEPzOGMGPKHevCnSo6&output=json&address=%s"
    val address = url.format("上海市")
    Source.fromURL(address)
      .getLines()
      .toStream
      .foreach(println)
  }

}
