package com.unionpay.test

import java.math.{BigDecimal => javaBigDecimal}
import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.unionpay.util.JsonUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.LocalDateTime

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by ywp on 2016/6/24.
  */
object MockData {

  private lazy val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("hello")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    val dealTrade = udf((lines: Seq[String]) => cleanTrade(lines))

    val tradeDF = Seq(
      ("user1", "card1", mockTimeStamp("2016-07-02")),
      ("user1", "card1", mockTimeStamp("2016-07-03")),
      ("user1", "card1", mockTimeStamp("2016-07-04")),
      ("user2", "card1", mockTimeStamp("2016-08-03")),
      ("user1", "card1", mockTimeStamp("2016-08-04")),
      ("user2", "card1", mockTimeStamp("2016-08-05")),
      ("user3", "card2", mockTimeStamp("2016-07-03")),
      ("user4", "card2", mockTimeStamp("2016-07-07")),
      ("user5", "card3", mockTimeStamp("2016-07-07")),
      ("user5", "card3", mockTimeStamp("2016-07-01"))
    ).toDF("uid", "cid", "date")
      .selectExpr("uid", "cid", "date_format(date,'yyyy-MM-dd') date")
      .mapPartitions(it => {
        it.map(r => {
          val cid = r.getAs[String]("cid")
          val uid = r.getAs[String]("uid")
          val date = r.getAs[String]("date")
          (cid, s"${uid}_${date}")
        })
      })
      .toDF("cid", "line")
      .groupBy($"cid").agg(dealTrade(collect_set("line")).as("lines"))
      .selectExpr("cid", "explode(lines) line")
      .selectExpr("cid", "line.uid uid", "line.`start` startTime", "line.`end` endTime")

    tradeDF.printSchema()
    tradeDF.show()
    sc.stop()
  }


  def mockTimeStamp(ds: String): Timestamp = {
    Timestamp.valueOf(LocalDateTime.fromDateFields(fm.parse(s"${ds} 00:00:00")).toString("yyyy-MM-dd HH:mm:ss"))
  }


  //todo fixed bug
  def cleanTrade(lines: Seq[String]): Seq[Line] = {
    val line = lines.map(_.split("_")).map {
      case Array(a, b) => (a, b)
    }.sortBy(_._2)
    val indexLine = line.zipWithIndex.map(_.swap)
    val res = for ((index, content) <- indexLine) yield {
      content match {
        case (uid, date) if index < line.size - 1 => {
          Line(uid, date, indexLine(index + 1)._2._2)
        }
        case (uid, date) if index == line.size - 1 => Line(uid, date, date)
      }
    }
    val tmpBuffer = ArrayBuffer[Line]()
    val buffer = ArrayBuffer(res: _*).zipWithIndex.map(_.swap)
    for ((index, l) <- buffer) {
      if (index < buffer.size - 1) {
        if (l.uid != buffer(index + 1)._2.uid) {
          var i = 0
          if (index == 0) {
            //
          } else {
            if (l.uid != buffer(index - 1)._2.uid) {
              i += 1
              tmpBuffer.append(l)
            } else {
              tmpBuffer.append(l.copy(start = buffer(i)._2.start))
            }
          }
        }
      } else {
        tmpBuffer.append(l)
      }
    }
    tmpBuffer
  }
}

case class Line(uid: String, start: String, end: String) {
  def _toString = s"${uid}_${start}_${end}"
}

