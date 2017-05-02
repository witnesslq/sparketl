package com.unionpay.es

import java.io.PrintWriter
import java.sql.Timestamp

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.unionpay.db.jdbc.JdbcUtil
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

/**
  * Created by ls on 2016/12/16.
  */
object BrandInfo2EsJob {


  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("BrandInfo2EsJob--品牌落地elasticsearch任务")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.yarn.driver.memoryOverhead", "1024")
      .set("spark.yarn.executor.memoryOverhead", "2000")
      //      .set("spark.speculation", "true")
      .set("spark.network.timeout", "300s")
      //todo 云主机 经常网络超时
      .set("spark.executor.heartbeatInterval", "50s")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .build("test", "nest", Option("shopNo"))
      .registerKryoClasses(Array(classOf[Location]))
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")


    val flag = udf((fs: Seq[String]) => {
      Option(fs) match {
        case None => "0"
        case Some(ff) => if (!ff.contains("00010000") || ff.isEmpty) "0" else "1"
      }
    })


    sqlContext.udf.register("json2Coupon", (cs: Seq[String]) => json2Coupon(cs))

    val getTop5 = udf((coupons: Seq[String]) => {
      Option(coupons) match {
        case None => null
        case Some(c) => if (c.length <= 5) c else c.take(5)
      }
    })

    //    sqlContext.udf.register("sortedCoupon", (couponList: Seq[CouponInfo]) => sortedCoupon(couponList))

    val couponDF = JdbcUtil.mysqlJdbcDF("tbl_content_coupon_inf", "sink")
      .filter("coupon_st='1' and date_format(current_date(),'yyyyMMdd')<=coupon_valid_dt_end and date_format(current_date(),'yyyyMMdd')>=coupon_listing_dt")
      .selectExpr("coupon_no couponNo", "coupon_nm couponName", "coupon_launch_bank_no", "substring(coupon_mchnt_service,5,1) as isSupportFlashPay",
        "substring(coupon_mchnt_service,6,1) as isForWallet", "substring(coupon_mchnt_service,4,1) exlusiveForNewer", "displayIndex", "coupon_listing_dt",
        "coupon_valid_dt_end", "flashPayType", "rec_upd_ts", "coupon_use_tm", "coupon_img", "coupon_sp", "coupon_type", "coupon_mchnt_service"
      )

    val shopDF = JdbcUtil.mysqlJdbcDF("tbl_content_shop_inf", "sink")
      .selectExpr("cast(shop_lnt as double) as shopLng", "cast(shop_lat as double) as shopLat", "shop_no as shopNo",
        "shop_nm shopName", "city_cd cityCd", "area_no areaNo", "brand_no brandNo", "industry_sub_no industrySubNo", "shop_valid_st")

    val shopCouponDF = JdbcUtil.mysqlJdbcDF("tbl_content_relate_shop_coupon", "sink")
      .selectExpr("coupon_no couponNo", "shop_no shopNo")

    val brandDF = JdbcUtil.mysqlJdbcDF("tbl_content_brand_inf", "sink")
      .selectExpr("brand_no as brandNo", "brand_nm as brandName", "brand_img", "brand_img_desc", "brand_desc", "displayIndex")


    val shopCouponTmpDF = couponDF.as('a)
      .join(shopCouponDF.as('b), $"a.couponNo" === $"b.couponNo", "left_outer")
      .selectExpr("a.*", "b.shopNo shopNo")

    val tmpDF = shopCouponTmpDF.as('a)
      .join(shopDF.as('b), $"a.shopNo" === $"b.shopNo", "left_outer")
      .selectExpr("a.*", "b.shopName", "b.cityCd", "b.areaNo", "b.industrySubNo", "b.shopLng", "b.shopLat", "b.brandNo", "b.shop_valid_st")
      .filter($"b.shop_valid_st" === "1")

    val tmpUnChangeDF = tmpDF.select("brandNo", "shopNo", "shopName", "cityCd", "areaNo", "industrySubNo", "shopLng", "shopLat")

    val tmpChangeDF = tmpDF.map(r => {

      val brandNo = r.getAs[String]("brandNo")
      val cityCd = r.getAs[String]("cityCd")

      val couponNo = r.getAs[String]("couponNo")
      val couponName = r.getAs[String]("couponName")
      val coupon_launch_bank_no = r.getAs[String]("coupon_launch_bank_no")
      val isSupportFlashPay = r.getAs[String]("isSupportFlashPay")
      val isForWallet = r.getAs[String]("isForWallet")
      val exlusiveForNewer = r.getAs[String]("exlusiveForNewer")
      val displayIndex = r.getAs[String]("displayIndex")
      val coupon_listing_dt = r.getAs[Timestamp]("coupon_listing_dt")
      val coupon_valid_dt_end = r.getAs[String]("coupon_valid_dt_end")
      val flashPayType = r.getAs[String]("flashPayType")
      val rec_upd_ts = r.getAs[Timestamp]("rec_upd_ts")
      val coupon_use_tm = r.getAs[String]("coupon_use_tm")
      val coupon_img = r.getAs[String]("coupon_img")
      val coupon_sp = r.getAs[String]("coupon_sp")
      val coupon_type = r.getAs[String]("coupon_type")
      val coupon_mchnt_service = r.getAs[String]("coupon_mchnt_service")
      val tmpCI = CouponInfo(couponNo, couponName, coupon_launch_bank_no, isSupportFlashPay, isForWallet, exlusiveForNewer, displayIndex,
        coupon_listing_dt, coupon_valid_dt_end, flashPayType, rec_upd_ts, coupon_use_tm, coupon_img, coupon_sp, coupon_type, coupon_mchnt_service)

      val couponInfo = coupon2Json(tmpCI)
      (brandNo, cityCd, coupon_launch_bank_no, couponInfo)
    }).toDF("brandNo", "cityCd", "coupon_launch_bank_no", "couponInfo")


    val brandCal = tmpChangeDF
      .groupBy($"brandNo", $"cityCd")
      .agg(flag(collect_set("coupon_launch_bank_no")).as("hasUnionpayCoupon"),
        getTop5(collect_set("couponInfo")).as("couponInfo"))
      .map(r => {
        val brandNo = r.getAs[String]("brandNo")
        val cityCd = r.getAs[String]("cityCd")
        val hasUnionpayCoupon = r.getAs[String]("hasUnionpayCoupon")

        val tmp = json2Coupon(r.getAs[Seq[String]]("couponInfo"))
        val couponInfo = sortedCoupon(tmp)

        (brandNo, cityCd, hasUnionpayCoupon, couponInfo)
      }).toDF("brandNo", "cityCd", "hasUnionpayCoupon", "couponInfo")

    val finalDF = tmpUnChangeDF.as('a)
      .join(brandDF.as('b), $"a.brandNo" === $"b.brandNo", "left_outer")
      .join(brandCal.as('c), $"a.brandNo" === $"c.brandNo" and ($"a.cityCd" === $"c.cityCd"), "left_outer")
      .selectExpr("a.*", "b.brandName brandName", "b.brand_img", "b.brand_img_desc", "b.brand_desc", "b.displayIndex",
        "c.hasUnionpayCoupon", "c.couponInfo couponInfo")
      .map(r => {
        val brandNo = r.getAs[String]("brandNo")
        val brandName = r.getAs[String]("brandName")
        val brand_img = r.getAs[String]("brand_img")
        val brand_img_desc = r.getAs[String]("brand_img_desc")
        val brand_desc = r.getAs[String]("brand_desc")
        val displayIndex = r.getAs[String]("displayIndex")
        val hasUnionpayCoupon = r.getAs[String]("hasUnionpayCoupon")

        val shopNo = r.getAs[String]("shopNo")
        val shopName = r.getAs[String]("shopName")
        val cityCd = r.getAs[String]("cityCd")
        val areaNo = r.getAs[String]("areaNo")

        val lat = r.getAs[Double]("shopLat")
        val lon = r.getAs[Double]("shopLng")
        val location = Location(lat = if (lat < -90 || lat > 90) 1 else lat, lon = if (lon < -180 || lon > 180) 1 else lon)
        val couponInfo = r.getAs[Seq[CouponInfo]]("couponInfo")

        (brandNo, brandName, brand_img, brand_img_desc, brand_desc, displayIndex, hasUnionpayCoupon, shopNo, shopName, cityCd, areaNo, location, couponInfo)
      }).toDF("brandNo", "brandName", "brand_img", "brand_img_desc", "brand_desc", "displayIndex", "hasUnionpayCoupon", "shopNo", "shopName", "cityCd", "areaNo", "location", "couponInfo")
      //昂贵的map操作之后最好 cache
      .persist(StorageLevel.MEMORY_AND_DISK)

    println(finalDF.count())

    val pw = new PrintWriter("C:\\Users\\ls\\Desktop\\数据\\show.txt")
    finalDF.printSchema()

    finalDF.filter($"hasUnionpayCoupon" === "1").select($"couponInfo").map(_.mkString("(", "|", ")")).collect()
      .foreach(pw.println)

    finalDF.saveToEs(sqlContext.sparkContext.getConf.getAll.toMap)

    sc.stop()
  }


  def sortedCoupon(couponList: Seq[CouponInfo]): Seq[CouponInfo] = {

    implicit val timeOrdering = new Ordering[CouponInfo] {

      override def compare(x: CouponInfo, y: CouponInfo): Int = {
        if (x.coupon_sp != y.coupon_sp)
          x.coupon_sp.compareTo(y.coupon_sp)
        else {
          val x_w = if (x.coupon_launch_bank_no == "00010000") 1 else 0
          val y_w = if (y.coupon_launch_bank_no == "00010000") 1 else 0

          if (x_w != y_w)
            y_w.compareTo(x_w)
          else {
            if (x.isSupportFlashPay != y.isSupportFlashPay)
              x.isSupportFlashPay.compareTo(y.isSupportFlashPay)
            else x.coupon_listing_dt.compareTo(y.coupon_listing_dt)
          }
        }
      }
    }
    couponList.sorted
  }


  def coupon2Json(couponInfo: CouponInfo): String = {

    lazy val objeMapper = new ObjectMapper()
    objeMapper.registerModule(DefaultScalaModule)
    val tt = objeMapper.writeValueAsString(couponInfo)
    tt
  }

  def json2Coupon(coupons: Seq[String]): Seq[CouponInfo] = {

    val pp = Option(coupons) match {
      case None => null
      case Some(ff) => {
        ff.map(r => {
          lazy val objeMapper = new ObjectMapper()
          objeMapper.registerModule(DefaultScalaModule)
          val tt = objeMapper.readValue(r, classOf[CouponInfo])
          tt
        })
      }
    }
    pp

  }

}


case class CouponInfo(couponNo: String, couponName: String, coupon_launch_bank_no: String, isSupportFlashPay: String, isForWallet: String,
                      exlusiveForNewer: String, displayIndex: String, coupon_listing_dt: Timestamp, coupon_valid_dt_end: String, flashPayType: String,
                      rec_upd_ts: Timestamp, coupon_use_tm: String, coupon_img: String, coupon_sp: String, coupon_type: String, coupon_mchnt_service: String
                     )