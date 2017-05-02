package com.unionpay.test.data

import java.util

import com.mongodb.{BasicDBList, BasicDBObject}
import com.unionpay.db.jdbc.{JdbcSaveMode, JdbcUtil}
import com.unionpay.db.mongo.MongoUtil._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.collection.JavaConversions._
import scala.sys.process._

/**
  * Created by ls on 2016/10/20.
  */
object DealArea {

  def delArea(implicit sqlContext: SQLContext) {
    import sqlContext.implicits._

    val areaDF = fetchAreaDF
    val shopDF = JdbcUtil.mysqlJdbcDF("tbl_content_shop_inf_20161129", "sink")
      .selectExpr("src_shop_no", "shop_lnt", "shop_lat")

    shopDF.repartition(50).write.mode(SaveMode.Overwrite).parquet("/tmp/ls/shop20161129")

    val tmpDF = sqlContext.read.parquet("/tmp/ls/shop20161129")
      .mapPartitions(it => {
        val tradingArea = C("TradingArea")
        it.map(row => {
          val myLocation = new BasicDBList()
          val lng = row.getAs[String]("shop_lnt")
          val lg = if (lng.trim.isEmpty) 0 else lng.trim.toDouble
          val lat = row.getAs[String]("shop_lat")
          val lt = if (lat.trim.isEmpty) 0 else lat.trim.toDouble
          myLocation.put(0, lg)
          myLocation.put(1, lt)
          val geoNear = new BasicDBObject("$geoNear",
            new BasicDBObject("near", myLocation)
              .append("limit", 1)
              .append("distanceField", "centerCoordinates")
              .append("maxDistance", 2000 / 6378137))
          val areaId = if (lg < 1) ""
          else {
            tradingArea.aggregate(util.Arrays.asList(geoNear))
              .useCursor(true)
              .iterator()
              .map(doc => {
                doc.getObjectId("_id").toString
              }).toStream.headOption getOrElse ("")
          }

          val shopId = row.getAs[String]("src_shop_no")
          (shopId, areaId)
        })
      }).toDF("src_shop_no", "areaId")


    val df = tmpDF.as('a)
      .join(areaDF.as('b), $"a.areaId" === $"b.src_area_no", "left_outer")
      .selectExpr("a.src_shop_no", "coalesce(b.area_no,'') area_no")

    JdbcUtil.saveToMysql("tbl_content_shop_inf_20161129", JdbcSaveMode.Upsert)(df)

    "hdfs dfs -rm -r /tmp/ls/shop20161129".!
  }

  def fetchAreaDF(implicit sqlContext: SQLContext): DataFrame = {
    val areaTB = "tbl_content_busi_area"
    JdbcUtil.mysqlJdbcDF(areaTB, "sink")
      .selectExpr("trim(area_no) area_no", "trim(src_area_no) src_area_no")
  }
}
