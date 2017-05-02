package com.scalatest

import org.scalatest.FunSuite

import scala.io.Source
import org.apache.spark.mllib.linalg.{Vector, Vectors, DenseVector => DV}

/**
  * Created by ywp on 2016/7/27.
  */
class ShopDistance extends FunSuite {

  test("计算商户和地标的距离") {
    //计算经纬度坐标两点距离
    def calculateDistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double, unit: String): Double = {
      val theta = lon1 - lon2
      var dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta))
      dist = Math.acos(dist)
      dist = rad2deg(dist)
      dist = dist * 60 * 1.1515
      unit.toLowerCase match {
        case "k" => dist * 1.609344
        case "n" => dist * 0.8684
      }
    }

    def deg2rad(deg: Double): Double = {
      deg * Math.PI / 180.0
    }

    def rad2deg(rad: Double): Double = {
      rad * 180 / Math.PI
    }

    val tangzhen = (116.93173, 36.6629)
    val guanglanlu = (116.93171, 36.66307)
    val guanglanlu2 = (116.93173, 36.6629)
    val guanglanlu3 = (116.93192, 36.66297)

    val dis = calculateDistance(tangzhen._1, tangzhen._2, guanglanlu._1, guanglanlu._2, "k")
    val dis2 = calculateDistance(tangzhen._1, tangzhen._2, guanglanlu2._1, guanglanlu2._2, "k")
    val dis3 = calculateDistance(tangzhen._1, tangzhen._2, guanglanlu3._1, guanglanlu3._2, "k")
    println(dis)
    println(dis2)
    println(dis3)

  }

  test("csv") {
    val total = Source.fromFile("D:\\project\\sparketl\\out\\artifacts\\sparketl_jar\\sim_shop.csv")("gbk")
      .getLines()
      .toStream
      .map(_.split(","))
      .map(x => Seq(x(1).replaceAll("\t", ""), x(5).replaceAll("\t", "")))
      .flatten
      .distinct
      .count(x => true)
    //      .foreach(println)
    println(total)
  }

  test("杭州字符串距离") {
    import com.unionpay.util.MathUtil._
    val a = "来伊份上海市闵行区古美西路三店"
    val b = "雷允上药房(浦东机场一店)"
    val t = calculateSimilarity2(b, a)
    println(t)

  }

  test("距离") {
    val x1 = Array(0.14821750243010898,
      0.07093251019636007,
      0.1095243973606838,
      -0.08427535259564303,
      -0.2649835704019152,
      0.2433517555577688,
      -0.4779118465519144,
      0.01352640170193131,
      -0.19902489437811122,
      0.12332758139823098,
      -0.18634974351023015,
      -0.010336127277780932,
      -0.3035374649486609,
      -0.2502299516418278,
      -0.1485649944679483,
      -0.29910557677388644,
      -0.16594464177774262,
      0.19924836814081184,
      0.06630040699886285,
      -0.11374781873764388,
      -0.10637151907092701,
      -0.006233566290294454,
      -0.03355164087979102,
      -0.022262908214575324,
      -0.22467431393864923,
      0.19170519837633812,
      -0.1106494981905066,
      -0.2031552277775327,
      -0.04031710342087103,
      -0.0051533375337209555)
    val x2 = Array(0.05630457337451084, 0.019918854770727506, 0.10473163728374213, -0.18882951830307806, -0.1872297867127683, -0.15890810302753766, 0.010356161925850763, 0.2091734173660024, 0.006557294067741524, 0.06050358200313649, -0.08329395905621552, -0.29678727653057, 0.06394780941910462, 0.45989284449823714, 0.21578862971354257, -0.1815718140624414, -0.09950262064765437, 0.02404568826570242, 0.2028407739541896, -0.26161039960775173, -0.20588666047006035, 0.0765029984880517, -0.15837542054701992, 0.11734931714996197, 0.22137129972172337, 0.09895845890805915, -0.11668189988275712, 0.3111534288057455, 0.19205781514393191, 0.19625942077556902)
    val v1 = Vectors.dense(x1).toSparse
    val v2 = Vectors.dense(x2).toSparse
    import com.unionpay.util.MathUtil._

    val sim = calculateSimilarity(v1, v2)
    println(sim)

  }
}