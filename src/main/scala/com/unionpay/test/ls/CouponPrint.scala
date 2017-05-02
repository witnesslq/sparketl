package com.unionpay.test.ls

import java.io.PrintWriter

import com.unionpay.db.jdbc.JdbcUtil
import com.unionpay.db.mongo.MongoUtil._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ywp on 2016/8/10.
  */
object CouponPrint {


  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("CouponPrint")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2000m")
      .set("spark.yarn.driver.memoryOverhead", "2048")
      .set("spark.yarn.executor.memoryOverhead", "2000")
      .set("spark.network.timeout", "300s")
      //todo 云主机 经常网络超时
      .set("spark.executor.heartbeatInterval", "30s")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "32")
    import sqlContext.implicits._


    val coupon = Seq(
      new CouponInfo("00010000", "1", "2006-11-30"),
      new CouponInfo("00010000", "3", "2007-11-30"),
      new CouponInfo("00010000", "3", "2003-11-30"),
      new CouponInfo("00011000", "0", "2007-04-30"),
      new CouponInfo("00020000", "1", "2016-03-02"),
      new CouponInfo("00040000", "0", "2026-02-19"),
      new CouponInfo("00050000", "0", "2096-10-21"),
      new CouponInfo("00060000", "3", "2008-11-11"),
      new CouponInfo("00060000", "1", "2008-11-11")
    ).toDF("coupon_bank","coupon_dic","coupon_listing_dt")

    coupon.printSchema()
    coupon.show()

    sc.stop()
  }


  def sortCoupon(couponList: Seq[CouponInfo]): Seq[CouponInfo] = {

    implicit val timeOrdering = new Ordering[CouponInfo] {

      override def compare(x: CouponInfo, y: CouponInfo): Int = {

        val x_w = if (x.coupon_bank == "00010000") 1 else 0
        val y_w = if (y.coupon_bank == "00010000") 1 else 0

        if (x_w != y_w)
          y_w.compareTo(x_w)
        else {
          if (x.coupon_dic != y.coupon_dic)
            x.coupon_dic.compareTo(y.coupon_dic)
          else x.coupon_listing_dt.compareTo(y.coupon_listing_dt)
        }
      }
    }
    couponList.sorted
  }

}

case class CouponInfo(coupon_bank: String, coupon_dic: String, coupon_listing_dt: String)
