package com.unionpay

import com.mongodb.client.model.IndexOptions
import com.mongodb.{BasicDBList, BasicDBObject}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

import scala.collection.JavaConversions._
import scala.collection.generic.SeqFactory

/**
  * Created by ywp on 2016/6/7.
  */
object MGOMainTest {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("mongodb test")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val table = "td"

    val df = Seq((1, Seq(121.591896, 31.210875)), (2, Seq(131.591995, 31.210906)), (3, Seq(121.591995, 31.210906)))
      .toDF("id", "location")

    df.save2Mongo(table)

    createOrDefault2dIndex(table)
    df.foreachPartition(it => {
      val col = C(table)
      //1.5公里
      it.foreach(r => {
        val myLocation = new BasicDBList()
        val id = r.getAs[Int]("id")
        val Seq(lng, lat) = r.getAs[Seq[Double]]("location")
        myLocation.put(0, lng)
        myLocation.put(1, lat)
        try {
          col.find(new BasicDBObject("location",
            new BasicDBObject("$near", myLocation).append("$maxDistance", 1500 / 6378137))
          )
            //mongo游标经常超时
            .noCursorTimeout(true)
            .iterator()
            .foreach(doc => {
              println((id, Seq(lng, lat)), doc.toJson)
            })
        } catch {
          case e: Exception => throw e
        }
      })
    })



    sc.stop()
  }
}