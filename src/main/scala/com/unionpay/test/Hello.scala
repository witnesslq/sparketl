package com.unionpay.test

import com.unionpay.es.{PlayLoadAddr, PlayLoadCount, ShopSuggest}
import com.unionpay.util.RedisOps._
import magellan.{Point, Polygon}
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

/**
  * Created by ywp on 2016/6/24.
  */
object Hello {

  def main(args: Array[String]) {
    println(args(0), args(0).getClass.getSimpleName)
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("hello")
      .buildRedis
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    val rdd = sc.keyRDD("landMark").getZSet()
    rdd.foreachPartition(it => {
      it.foreach {
        case (k, v) => println(k, v)
      }
    })
    sc.stop()
  }

  def readES(implicit sqlContext: SQLContext) = {
    val es = sqlContext.esDF(sqlContext.sparkContext.getConf.getAll.toMap)
    es.printSchema()
    es.show()
  }

  def save2ES(implicit sqlContext: SQLContext) = {
    //    "suggest" : {
    //      "input": [ "商圈名" ],
    //      "output": "商圈名",
    //      "payload" : { "type" :"02","id":”商圈id" },
    //      "weight" : 20
    //    },
    //content_suggest
/*    import sqlContext.implicits._
    val mockDF = Seq((5, "肯德基", "肯德基非常好"), (6, "肯打继", "肯打继打一通"))
      .toDF("shopId", "name", "content")
      .map(row => {
        val id = row.getAs[Int]("shopId")
        val name = row.getAs[String]("name")
        val content = row.getAs[String]("content")
        val nameInput = Seq(name)
        val nameOutput = name
        val namePlayLoad = PlayLoadCount("01", s"name_$id",1L)
        val nameSuggest = ShopSuggest(nameInput, nameOutput, namePlayLoad)
        val contentInput = Seq(content)
        val contentOutput = content
        val contentPlayLoad = PlayLoadCount("02", s"content_$id",1L)
        val contentSuggest = Suggest(contentInput, contentOutput, contentPlayLoad)
        (id, name, content, nameSuggest, contentSuggest, contentSuggest, contentSuggest, contentSuggest)
      }).toDF("shopId", "name", "content", "shop_suggest", "brand_suggest")
    //    mockDF.saveToEs(resource, sqlContext.sparkContext.getConf.getAll.toMap)
    mockDF.saveToEs(sqlContext.sparkContext.getConf.getAll.toMap)*/
  }


  def test(sc: SparkContext)(implicit sqlContext: SQLContext): Unit = {
    import sqlContext.implicits._
    val points = sc.parallelize(Seq((121.60843, 31.21176), (121.608782, 31.211157), (121.60582, 31.211519), (121.608546, 31.211073)))
      .toDF("x", "y").select(point($"x", $"y").as("point"))
    points.printSchema()
    points.show()

    val ring = Array(Point(121.607116, 31.21277), Point(121.607087, 31.211588), Point(121.607924, 31.210465),
      Point(121.609103, 31.21077),
      Point(121.609893, 31.211134),
      Point(121.609988, 31.212755), Point(121.607116, 31.21277))
    val polygons = sc.parallelize(Seq(
      PolygonRecord(Polygon(Array(0), ring))
    )).toDF()
    polygons.show()

    polygons.join(points).where($"point" within $"polygon")
      .selectExpr("polygon", "point")
      .map {
        case Row(polygon: Polygon, point: Point) => (polygon.xcoordinates, polygon.ycoordinates, point.getX(), point.getY())
      }
      .toDF("xcoordinates", "ycoordinates", "x", "y")
      .show()
    //    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
    //      Point(-1.0, -1.0), Point(-1.0, 1.0),
    //      Point(1.0, 1.0))
    //    val polygons = sc.parallelize(Seq(
    //      PolygonExample(Polygon(Array(0), ring))
    //    )).toDF()
    //
    //    val points = sc.parallelize(Seq(
    //      PointExample(Point(0.0, 0.0)),
    //      PointExample(Point(2.0, 2.0))
    //    )).toDF()
    //
    //    val joined = points.join(polygons).where($"polygon" >? $"point")
    //    joined.printSchema()
    //    joined.show()
  }

  //计算经纬度坐标两点距离
  def calculateDistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double, unit: String): Double = {
    val theta = lon1 - lon2
    var dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta))
    dist = Math.acos(dist)
    dist = rad2deg(dist)
    dist = dist * 60 * 1.1515
    unit.toLowerCase match {
      case "k" => dist * 1.609344
      case "n" => dist * 0.8684
    }
  }

  def deg2rad(deg: Double): Double = {
    deg * Math.PI / 180.0
  }

  def rad2deg(rad: Double): Double = {
    rad * 180 / Math.PI
  }
}

case class PolygonRecord(polygon: Polygon)

case class PointExample(point: Point)

case class PolygonExample(polygon: Polygon)


//case class Suggests(name_suggest: Suggest, content_suggest: Suggest)