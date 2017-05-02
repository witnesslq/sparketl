package com.unionpay.test

import java.io.PrintWriter

import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import com.unionpay.db.jdbc.JdbcUtil._

/**
  * Created by ywp on 2016/8/10.
  */
object ExportShopBuzHour {

  private lazy val timeRegex ="""\d{1,2}(:|：)\d{2}""".r
  private lazy val dianRegex ="""\d{1,2}点""".r
  private lazy val chRegex ="""[\u4e00-\u9fa5]""".r

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("ExportOldShop")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "25")
    import sqlContext.implicits._
    implicit val strOrdering = new Ordering[String] {
      override def compare(x: String, y: String): Int = {
        x.split(":").head.toInt.compareTo(y.split(":").head.toInt)
      }
    }
    //todo
    val timeIt = udf((oHour: String) => {
      val name = oHour.replaceAll(" ", "").replaceAll("：", ":")
      val finalName = if (name == "24" || name == "二十四小时" || name.contains("24小时")) "00:00-23:59"
      else {
        val timeStr = timeRegex.findAllIn(name).toStream
        if (timeStr.isEmpty) {
          val dianStr = dianRegex.findAllIn(name).toStream
          if (dianStr.isEmpty || dianStr.size == 1) name
          else {
            dianStr.take(2).map(_.replaceAll("点", "")).toList match {
              case Nil => name
              case head :: second :: Nil => s"$head:00-${if (second.toInt > 10) second else second.toInt + 12}:00"
            }
          }
        }
        else {
          val mm = timeStr.sorted
          val min = mm.min
          val max = mm.max
          //if (min == max) s"$min-${max + 12}" else s"$min-$max"
          s"$min-$max"
        }
      }
      if (chRegex.findFirstIn(finalName).isDefined) "具体以店内公布为准" else finalName
    })
    val pw = new PrintWriter("/tmp/sb.cvs", "utf-8")
    val shopDF = mysqlJdbcDF("tbl_chmgm_preferential_mchnt_inf")
      .selectExpr("trim(BUSS_HOUR) BUSS_HOUR")
      .filter($"BUSS_HOUR".rlike("""[\u4e00-\u9fa5]"""))
      .select(trim(regexp_replace($"BUSS_HOUR", """[()（）a-zA-Z。.；]""", "")).as("BUSS_HOUR"))
      .distinct()
      .select(timeIt($"BUSS_HOUR"))
      .map(_.mkString(","))
      .collect()
      .foreach(pw.println)
    pw.close()

    sc.stop()
  }

}
