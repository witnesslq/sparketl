package com.unionpay.es

import com.unionpay.db.jdbc.JdbcUtil._
import com.unionpay.db.jdbc.MysqlConnection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

/**
  * -XX:+UseG1GC -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
  * -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -Xms88g -Xmx88g
  * -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThread=20
  *
  * Created by ywp on 2016/7/25.
  */
object ShopInfo2EsJob {

  //  private lazy val landMarkHdfsPath = "/test/landMark"
  private lazy val landMarkHdfsPath = "/test/landMark"

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("ShopInfo2EsJob--商户落地elasticsearch任务")
      //      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.yarn.driver.memoryOverhead", "1024")
      .set("spark.yarn.executor.memoryOverhead", "2000")
      //      .set("spark.speculation", "true")
      .set("spark.network.timeout", "300s")
      //todo 云主机 经常网络超时
      .set("spark.executor.heartbeatInterval", "50s")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
      .build("content", "shop", Option("shopId"))
      .registerKryoClasses(Array(classOf[PlayLoadAddr], classOf[PlayLoadCount], classOf[Location], classOf[BrandSuggest], classOf[ShopSuggest]))
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "32")
    // 包含1 就1
    val flag = udf((fs: Seq[String]) => {
      Option(fs) match {
        case None => "0"
        case Some(ff) => if (!ff.contains("1") || ff.isEmpty) "0" else "1"
      }
    })
    val hasCoupon = udf((cs: Seq[String]) => {
      Option(cs) match {
        case None => false
        case Some(cc) => if (cc.isEmpty) false else true
      }
    })

    val mysql = MysqlConnection.build("mysql2mysql", "sink")
    import mysql._
    val shopDF = sqlContext.jdbcDF("tbl_content_shop_inf")
      .selectExpr("cast(shop_lnt as double) as shopLng", "cast(shop_lat as double) as shopLat", "shop_no as shopNo", "shop_addr shopAddr",
        "shop_nm as shopName", "city_cd as cityCd", "area_no as areaNo", "brand_no as brandNo", "county_cd as countyCd",
        "industry_no as industryNo", "industry_sub_no as industrySubNo", "landmark_id landmarkId"
      )

    //todo shopEs添加AreaId，AreaName，AreaEs对AreaId，AreaName重命名
    val areaDF = sqlContext.jdbcDF("tbl_content_busi_area").selectExpr("area_no as areaNo", "area_nm as areaName")
    val brandDF = sqlContext.jdbcDF("tbl_content_brand_inf").selectExpr("brand_no as brandNo", "brand_nm as brandName")
    val couponDF = sqlContext.jdbcDF("tbl_content_coupon_inf")
      .selectExpr("coupon_no as couponNo", "coupon_launch_bank_no as couponLaunchBankNo", "coupon_supp_flashpay as isSupportFlashPay", "coupon_for_wallet as isForWallet")

    val shopCouponTMPDF = sqlContext.jdbcDF("tbl_content_relate_shop_coupon")
      .selectExpr("coupon_no as couponNo", "shop_no as shopNo")

    val shopCouponDF = shopCouponTMPDF.as('a)
      .join(couponDF.as('b), $"a.couponNo" === $"b.couponNo")
      .selectExpr("a.shopNo", "b.couponNo", "b.isSupportFlashPay", "b.isForWallet")

    val shopBankDF = shopCouponDF.as('a)
      .join(couponDF.as('b), $"a.couponNo" === $"b.couponNo")
      .selectExpr("b.couponLaunchBankNo", "a.shopNo")
      .groupBy($"shopNo").agg(collect_set($"couponLaunchBankNo").as("couponLaunchBankNo"))


    val shopFlagDF = shopCouponDF
      .groupBy($"shopNo")
      .agg(flag(collect_set("isSupportFlashPay")).as("isSupportFlashPay"),
        flag(collect_set("isForWallet")).as("isForWallet"),
        hasCoupon(collect_set("couponNo")).as("isExistCoupon"),
        countDistinct("couponNo").as("couponCount")
      )

    val tmpBrandShopDF = shopDF.selectExpr("shopNo", "brandNo")
      .groupBy("brandNo").agg(countDistinct("shopNo").as("shopCount"))

    val brandShopDF = brandDF.as("a")
      .join(tmpBrandShopDF.as('b), $"a.brandNo" === $"b.brandNo", "left_outer")
      .selectExpr("a.brandNo", "a.brandName", "coalesce(b.shopCount,0) shopCount")

    val landMarkDF = sqlContext.read.parquet(landMarkHdfsPath).selectExpr("landmarkId", "landmarkName")
    val tDF = shopDF.as('a)
      .join(broadcast(areaDF).as('b), $"a.areaNo" === $"b.areaNo", "left_outer")
      .join(brandShopDF.as('c), $"a.brandNo" === $"c.brandNo", "left_outer")
      .join(shopBankDF.as('d), $"a.shopNo" === $"d.shopNo", "left_outer")
      .join(shopFlagDF.as('e), $"e.shopNo" === $"a.shopNo", "left_outer")
      .join(broadcast(landMarkDF).as('f), $"a.landmarkId" === $"f.landmarkId", "left_outer")
      .select($"a.shopLat", $"a.shopLng", $"a.shopNo".as("shopId"), $"a.shopName", $"a.shopAddr",
        $"a.areaNo".as("areaId"), $"a.brandNo".as("brandId"), $"a.cityCd",
        coalesce($"b.areaName", lit("")).as("areaName"),
        coalesce($"c.shopCount", lit(0)).as("shopCount"),
        coalesce($"c.brandName", lit("")).as("brandName"), $"a.industryNo", $"a.industrySubNo", $"d.couponLaunchBankNo", $"a.countyCd",
        coalesce($"e.isExistCoupon", lit(false)).as("isExistCoupon"), coalesce($"e.isSupportFlashPay", lit("0")).as("isSupportFlashPay"),
        coalesce($"e.isForWallet", lit("0")).as("isForWallet"), coalesce($"e.couponCount", lit(0l)).as("couponCount"),
        $"a.landmarkId", coalesce($"f.landmarkName", lit("")).as("landmarkName")
      )


    val sDF = tDF.distinct()

    addSuggests(sDF)

    sc.stop()
  }

  def addSuggests(shopDF: DataFrame)(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    val esDF = shopDF.mapPartitions(
      it => {
        it.map(
          row => {
            val shopId = row.getAs[String]("shopId")
            val shopName = row.getAs[String]("shopName")
            val shopAddr = row.getAs[String]("shopAddr")

            val lat = row.getAs[Double]("shopLat")
            val lon = row.getAs[Double]("shopLng")
            val location = Location(lat = if (lat < -90 || lat > 90) 1 else lat, lon = if (lon < -180 || lon > 180) 1 else lon)

            val locationJsonStr =
              s"""
                 |{"lat":${if (lat < -90 || lat > 90) 1 else lat},"lon":${if (lon < -180 || lon > 180) 1 else lon}}
               """.stripMargin

            val areaId = row.getAs[String]("areaId")
            val areaName = row.getAs[String]("areaName")
            /*val areaPlayLoad = PlayLoad("02", areaId)
            val areaSuggest = Suggest(Seq(areaName), areaName, areaPlayLoad, 20)*/

            val brandId = row.getAs[String]("brandId")
            val brandName = row.getAs[String]("brandName")
            val shopCount = row.getAs[Long]("shopCount")
            val brandPlayLoad = PlayLoadCount("01", brandId, shopCount)
            val brandSuggest = BrandSuggest(Seq(brandName), brandName, brandPlayLoad)

            val shopPlayLoad = PlayLoadAddr("04", shopId, shopAddr, brandId, locationJsonStr)
            val shopSuggest = ShopSuggest(Seq(shopName), shopName, shopPlayLoad)

            val landmarkId = row.getAs[String]("landmarkId")
            val landmarkName = row.getAs[String]("landmarkName")
            /* val landmarkPlayLoad = PlayLoad("03", landmarkId)
            val landmarkSuggest = Suggest(Seq(landmarkName), landmarkName, landmarkPlayLoad, 10)*/

            val cityCd = row.getAs[String]("cityCd")
            val countyCd = row.getAs[String]("countyCd")
            val industryNo = row.getAs[String]("industryNo")
            val industrySubNo = row.getAs[String]("industrySubNo")

            val couponLaunchBankNo = row.getAs[Seq[String]]("couponLaunchBankNo")
            val isExistCoupon = row.getAs[Boolean]("isExistCoupon")
            val isSupportFlashPay = row.getAs[String]("isSupportFlashPay")
            val isForWallet = row.getAs[String]("isForWallet")

            val couponCount = row.getAs[Long]("couponCount")

            (shopId, shopName, shopAddr, brandId, brandName, couponCount, areaId, areaName, landmarkId, landmarkName, cityCd, countyCd,
              industryNo, industrySubNo, location, couponLaunchBankNo, isExistCoupon, isSupportFlashPay, isForWallet,
              brandSuggest, /*areaSuggest, landmarkSuggest,*/ shopSuggest)
          }
        )
      }).toDF("shopId", "shopName", "shopAddr", "brandId", "brandName", "couponCount", "areaId", "areaName", "landmarkId", "landmarkName", "cityCd", "countyCd",
      "industryNo", "industrySubNo", "location", "couponLaunchBankNo", "isExistCoupon", "isSupportFlashPay", "isForWallet",
      "brand_suggest", /*"area_suggest",  "landmark_suggest",*/ "shop_suggest")
      //昂贵的map操作之后最好 cache
      .persist(StorageLevel.MEMORY_AND_DISK)

    esDF.saveToEs(sqlContext.sparkContext.getConf.getAll.toMap)

  }


}


case class PlayLoadCount(`type`: String, id: String, shopCount: Long)

case class PlayLoadAddr(`type`: String, id: String, shopAddr: String, brandId: String, location: String)

case class BrandSuggest(input: Seq[String], output: String, payload: PlayLoadCount, weight: Option[Int] = None)

case class ShopSuggest(input: Seq[String], output: String, payload: PlayLoadAddr, weight: Option[Int] = None)

case class Location(lat: Double, lon: Double)