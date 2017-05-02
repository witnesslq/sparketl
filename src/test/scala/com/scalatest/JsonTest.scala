package com.scalatest

import com.unionpay.streaming.HotWord
import com.unionpay.util.JsonUtil
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

/**
  * Created by ywp on 2016/9/8.
  */
class JsonTest extends FunSuite {

  case class Test(id: Int, name: String)

  test("json array string to list class") {
    import com.unionpay.util.JsonUtil._
    val ts = Seq(Test(1, "a"), Test(2, "b"))
    val as = toJacksonString(ts)
    println(as)
    val xx = readAsBeanByJson4s[Seq[Test]](as)
    println(xx)

  }

  test("string contains array of string") {
    val jsonKeys = Seq("userId", "type", "id", "brandId", "value", "cityCd", "input")
    val regex = s"""${jsonKeys.mkString("[", " ", "]")}""".r
    val str =
      """
        |{"userId", "type", "id", "brandId", "value", "cityCd", "input"}
      """.stripMargin

    val x = jsonKeys.toStream.par.map(str.contains).reduce((x, y) => x && y)
    println(x)

    val y = regex.findAllIn(str)
    println(y)

  }

  test("json array") {

    /* implicit val msgOrdering = new Ordering[HotWord] {
       override def compare(x: HotWord, y: HotWord): Int = x.msgTs.compare(y.msgTs)
     }*/

    val js =
      """[{"brandId":"BRND01e9d6af8dbb43c89740a95b9171a27d","cityCd":"310000","id":"SHOP297b74fd08ee4035a2053bf80088ae1d","input":"肯德基1","type":"05","userId":"15800524201","value":"肯德基安亭店-洗手间","msgTs":1},
         {"brandId":"89935","cityCd":"310000","id":"SHOPc718af28667a4b7588d4cf7e3f7a9214","input":"肯德基2","type":"05","userId":"15800524201","value":"肯德基展示(上海肯德基成山店SHA240)","msgTs":2},
         {"brandId":"89935","cityCd":"310000","id":"SHOP196e91f67bb54554a8501eb6c5a21e3e","input":"肯德基3","type":"05","userId":"15800524201","value":"肯德基展示(上海肯德基上海滩店SHA307)","msgTs":3},
         {"brandId":"89935","cityCd":"310000","id":"SHOP196e91f67bb54554a8501eb6c5a21e3e","input":"肯德基3","type":"05","userId":"15800524201","value":"肯德基展示(上海肯德基上海滩店SHA307)","msgTs":4}
         ]
      """

    val hts = JsonUtil.readAsBeanByJson4s[Seq[HotWord]](js)

    hts.groupBy(_.input.trim).mapValues(_.head).values.toSeq.sortBy(-_.msgTs.getOrElse(0L)) /*.map(_.searchJsonStringValue)*/ .foreach(println)
    hts.groupBy(_.input.trim).mapValues(_.head).values.toSeq.sortBy(-_.msgTs.getOrElse(0L)) /*.map(_.searchJsonStringValue)*/ .foreach(println)

  }

  test("json hotword") {

    val json =
      """
        |{"brandId":"74763","cityCd":"310000","id":"74763","input":"小肥羊","msgTs":1474170261605,"type":"01","userId":"15800524201","value":"小肥羊"}
      """.stripMargin
    val ht = JsonUtil.readAsBeanByJson4s[HotWord](json)
    println(ht)


  }

}

