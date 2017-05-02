package com.unionpay.test.online

import com.unionpay._
import com.unionpay.etl._
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import com.unionpay.db.mongo.MongoUtil._

/**
  * Created by ls on 2016/12/12.
  */
object LandMark {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("LandMark--地标信息任务")
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
    sqlContext.setConf("spark.sql.shuffle.partitions", spark_shuffle_partitions)
    etl
    sc.stop()
  }

  def etl(implicit sqlContext: SQLContext) = {

    import sqlContext.implicits._
    val landMarkDF = sqlContext.mongoDF("Landmark")
      .selectExpr("_id.oid landmark_no", "provinceCode prov_cd", "cityCode city_cd", "countyCode county_cd",
        "landMark landmark_nm", "centerCoordinates location")
      .map(r => {
        val landmark_no = r.getAs[String]("landmark_no")
        val prov_cd = r.getAs[String]("prov_cd")
        val city_cd = r.getAs[String]("city_cd")
        val county_cd = r.getAs[String]("county_cd")
        val landmark_nm = r.getAs[String]("landmark_nm")
        val Seq(lon, lat) = r.getAs[Seq[Double]]("location")
        val landmark_lnt = if (lon > 180 || lon < -180) 1 else lon
        val landmark_lat = if (lat > 90 || lat < -90) 1 else lat
        (landmark_no, landmark_nm, landmark_lnt, landmark_lat, prov_cd, city_cd, county_cd)
      }).toDF("landmark_no", "landmark_nm", "landmark_lnt", "landmark_lat", "prov_cd", "city_cd", "county_cd")
      .addAlwaysColumn

    landMarkDF.repartition(4).write.mode(SaveMode.Overwrite).parquet("/tmp/ls/landMark")

  }
}
