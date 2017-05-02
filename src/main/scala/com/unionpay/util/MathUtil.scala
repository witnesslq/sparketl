package com.unionpay.util

import org.apache.spark.mllib.linalg.{DenseVector => DV, SparseVector => SV}

/**
  * Created by ywp on 2016/8/24.
  */
object MathUtil {

  //计算余弦相似度
  def calculateSimilarity(v1: SV, v2: SV): Double = {
    import breeze.linalg._
    val bsv1 = new SparseVector[Double](v1.indices, v1.values, v1.size)
    val bsv2 = new SparseVector[Double](v2.indices, v2.values, v2.size)
    val sim = (bsv1.dot(bsv2) / (norm(bsv1) * norm(bsv2)))
    sim.formatted("%.6f").toDouble
  }

  //字符串公共部分
  def calculateSimilarity2(unionSN: String, crawlSN: String): Double = {
    val unionShopName = replaceStr(unionSN)
    val crawlShopName = replaceStr(crawlSN)
    val interSize = unionShopName.intersect(crawlShopName).size.toFloat / Math.max(unionShopName.size, crawlShopName.size)
    "%.3f".format((1 - interSize) * 100).toDouble
  }

  private[MathUtil] def replaceStr(name: String): String = {
    name.replaceAll("[（）() ]", "")
  }
}
