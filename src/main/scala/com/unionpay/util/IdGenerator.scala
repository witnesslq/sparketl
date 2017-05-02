package com.unionpay.util

import java.text.DecimalFormat
import java.util.UUID

/**
  * Created by ywp on 2016/7/11.
  */
object IdGenerator {

  def generateShopID: String = {
    s"SHOP${UUID.randomUUID().toString.replaceAll("-", "")}"
  }

  def generateCouponID: String = {
    s"COPN${UUID.randomUUID().toString.replaceAll("-", "")}"
  }

  def generateGoodsID: String = {
    s"GOOD${UUID.randomUUID().toString.replaceAll("-", "")}"
  }

  def generateCommentID: String = {
    s"CMMT${UUID.randomUUID().toString.replaceAll("-", "")}"
  }

  def generateBrandID: String = {
    s"BRND${UUID.randomUUID().toString.replaceAll("-", "")}"
  }

  def generateAreaID: String = {
    s"AREA${UUID.randomUUID().toString.replaceAll("-", "")}"
  }

}
