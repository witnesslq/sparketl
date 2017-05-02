package com.unionpay.test.ls

import scala.io.Source

/**
  * Created by ls on 2016/11/29.
  */
object ReadFile {

  def main(args: Array[String]) {
    val file = Source.fromFile("C:\\Users\\ls\\Desktop\\20161128\\update_shop_coupon_relation.sql")
      .getLines()
      .toArray

    file.take(10).foreach(println)

    println(file.size)

    println(file.distinct.size)
  }

}
