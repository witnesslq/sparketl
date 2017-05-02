package com.unionpay.util

/**
  * Created by ywp on 2016/8/12.
  */
object GeoDistance{

  //计算经纬度坐标两点距离
  def calculateDistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double, unit: String): Double = {
    val theta = lon1 - lon2
    var dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta))
    dist = Math.acos(dist)
    dist = rad2deg(dist)
    dist = dist * 60 * 1.1515
    unit.toLowerCase match {
      //千米
      case "k" => dist * 1.609344
      //米
      case "m" => dist * 1.609344 * 1000
      //海里
      case "n" => dist * 0.8684
    }
  }

  def deg2rad(deg: Double): Double = {
    deg * Math.PI / 180.0
  }

  def rad2deg(rad: Double): Double = {
    rad * 180 / Math.PI
  }

}
