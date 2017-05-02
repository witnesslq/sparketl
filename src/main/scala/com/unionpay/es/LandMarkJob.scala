package com.unionpay.es

import com.unionpay.db.mongo.MongoUtil._
import magellan.Point
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

/**
  * Created by ywp on 2016/7/25.
  */
object LandMarkJob {

  private lazy val filePath = "/test/landMark"

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("LandMarkJob--地标任务")
      //      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Location]))
      .build("hotlandmark", "landmark", Option("landmarkId"))
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "24")
    saveLand
    sc.stop()
  }

  def saveLand(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    val mongoDF = sqlContext.mongoDF("Landmark")
      .selectExpr("_id.oid landmarkId", "landMark landmarkName", "centerCoordinates location")

    val df = mongoDF
      .mapPartitions(it => {
        it.map(row => {
          val landmarkId = row.getAs[String]("landmarkId")
          val landmarkName = row.getAs[String]("landmarkName")
          val Seq(lon, lat) = row.getAs[Seq[Double]]("location")
          val landPlayLoad = PlayLoad("03", landmarkId)
          val landSuggest = Suggest(Seq(landmarkName), landmarkName, landPlayLoad)
          (landmarkId, landmarkName, Location(lat = if (lat > 90 || lat < -90) 1 else lat, lon = if (lon > 180 || lon < -180) 1 else lon), landSuggest)
        })
      }).toDF("landmarkId", "landmarkName", "location", "landmark_suggest")

    df.repartition(4).write.mode(SaveMode.Overwrite).parquet(filePath)

    df.saveToEs(sqlContext.sparkContext.getConf.getAll.toMap)
  }

}

case class PlayLoad(`type`: String, id: String)

case class Suggest(input: Seq[String], output: String, payload: PlayLoad, weight: Option[Int] = None)