package com.unionpay.es

import com.unionpay.db.jdbc.JdbcUtil._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

/**
  * Created by ywp on 2016/9/12.
  */
object CouponInfo2EsJob {


  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("CouponInfo2EsJob--优惠")
      //      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .registerKryoClasses(Array(classOf[Location]))
      .build("hotcoupon", "coupon", Option("mapId"))
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    //todo 本地测试
    sqlContext.setConf("spark.sql.shuffle.partitions", "32")

    import sqlContext.implicits._
    val couponDF = mysqlJdbcDF("tbl_content_coupon_inf", "sink")
      // todo 过滤条件，后期加入
      //      .filter("coupon_st='1' and date_format(current_date(),'yyyyMMdd')<=coupon_valid_dt_end")
      .selectExpr("coupon_no")
      //todo 数据膨胀
      .repartition(32, $"coupon_no")


    val couponShopDF = mysqlJdbcDF("tbl_content_relate_shop_coupon", "sink")
      .selectExpr("coupon_no", "shop_no", "row_id")
      .repartition(32, $"coupon_no")


    val validCouponDF = couponShopDF.as('a)
      .join(couponDF.as('b), $"a.coupon_no" === $"b.coupon_no", "leftsemi")
      .selectExpr("a.coupon_no", "a.shop_no", "a.row_id")


    val shopDF = mysqlJdbcDF("tbl_content_shop_inf", "sink")
      //todo 测试用例
      //      .filter($"shop_no" === "SHOP99cfe6f9aae240da8bdcc97bfb858130")
      .selectExpr("shop_no", "industry_sub_no", "case when shop_lnt='' then 0 else cast(shop_lnt as double) end lng",
      "case when shop_lat='' then 0 else cast(shop_lat as double) end lat", "industry_sub_no", "city_cd"
    )
      //todo 经纬度非法时设置为 1
      .selectExpr(
      "shop_no", "city_cd", "industry_sub_no", "case when lng>180 then 1 when lng<-180 then 1 else lng end lng",
      "case when lat>90 then 1 when lat<-90 then 1 else lat end lat", "industry_sub_no"
    )
      .repartition(32, $"shop_no")


    val esCouponDF = validCouponDF.as('a)
      .join(shopDF.as('b), $"a.shop_no" === $"b.shop_no")
      .selectExpr("a.row_id  mapId", "a.shop_no shopId", "coalesce(b.city_cd,'') cityCd", "a.coupon_no couponId", "b.lng", "b.lat", "industry_sub_no industrySubNo")
      .mapPartitions(it => {
        it.map(row => {
          val couponId = row.getAs[String]("couponId")
          val industrySubNo = row.getAs[String]("industrySubNo")
          val shopId = row.getAs[String]("shopId")
          val cityCd = row.getAs[String]("cityCd")
          val lon = row.getAs[Double]("lng")
          val lat = row.getAs[Double]("lat")
          val mapId = row.getAs[Int]("mapId")
          (mapId, couponId, industrySubNo, shopId, cityCd, Location(lat, lon))
        })
      }).toDF("mapId", "couponId", "industrySubNo", "shopId", "cityCd", "location")


    esCouponDF.saveToEs(sc.getConf.getAll.toMap)

    sc.stop()
  }

}
