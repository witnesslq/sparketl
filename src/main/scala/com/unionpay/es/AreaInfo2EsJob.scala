package com.unionpay.es

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import com.unionpay.db.jdbc.JdbcUtil._
import org.elasticsearch.spark.sql._

/**
  * Created by ls on 2016/9/7.
  */
object AreaInfo2EsJob {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("AreaInfo2EsJob--商圈任务")
      //      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[PlayLoadCount], classOf[AreaSuggest]))
      .build("area", "area", Option("areaId"))
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "24")
    saveArea
    sc.stop()
  }

  def saveArea(implicit sqlContext: SQLContext) = {

    import sqlContext.implicits._

    val areaDF = mysqlJdbcDF("tbl_content_busi_area", "sink").selectExpr("area_no as areaNo", "area_nm as areaName")

    //todo 数据倾斜
    val shopDF = mysqlJdbcDF("tbl_content_shop_inf", "sink").selectExpr("area_no areaNo", "shop_no shopNo")
      .repartition(6, $"areaNo")
      .groupBy("areaNo").agg(countDistinct("shopNo").as("shopCount"))

    val areaEsDF = areaDF.as("a")
      .join(shopDF.as('b), $"a.areaNo" === $"b.areaNo", "left_outer")
      .selectExpr("a.areaNo", "a.areaName", "coalesce(b.shopCount,0) shopCount")


    val tmpDF = areaEsDF.mapPartitions(
      it => {
        it.map(row => {
          val areaId = row.getAs[String]("areaNo")
          val areaName = row.getAs[String]("areaName")
          val shopCount = row.getAs[Long]("shopCount")
          val areaPlayLoad = PlayLoadCount("02", areaId, shopCount)
          val areaSuggest = AreaSuggest(Seq(areaName), areaName, areaPlayLoad)

          (areaId, areaName, shopCount, areaSuggest)
        })
      }).toDF("areaId", "areaName", "shopCount", "areaSuggest")

    tmpDF.saveToEs(sqlContext.sparkContext.getConf.getAll.toMap)
  }
}


case class AreaSuggest(input: Seq[String], output: String, payload: PlayLoadCount, weight: Option[Int] = None)
