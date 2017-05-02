package com.scalatest

import ch.hsr.geohash.GeoHash
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by ywp on 2016/8/7.
  */
class GeoHashTest extends FunSuite with BeforeAndAfterAll {


  test("经纬度转为geoHash") {
    //    val hash = new GeoHash()
    val lg = (119.58765, 39.94575)
    val geoHash = GeoHash.geoHashStringWithCharacterPrecision(lg._2, lg._1, 12)
    println(geoHash)
  }


}
