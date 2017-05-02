package com.unionpay.test.data

import java.io.File

import com.databricks.spark.csv._
import com.unionpay.db.jdbc.JdbcUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/9/24.
  */
object TestCsvFile {

  private lazy val timeRegex ="""\d{1,2}(:|：)\d{2}""".r
  private lazy val dianRegex ="""\d{1,2}点""".r
  private lazy val chRegex ="""[\u4e00-\u9fa5]""".r

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("InsertData to CMS === 插入数据")
      //      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "32")
    sqlContext.udf.register("unionValue", (value: String, new_value: String) => unionValue(value, new_value))
    sqlContext.udf.register("parkInfo", (flashPay: String, free: String, wifi: String, park: String, card: String) => "" + flashPay + free + wifi + park + card)
    sqlContext.udf.register("getShopValidSt", (shopName: String, n: String) => getShopValidSt(shopName, n))
    sqlContext.udf.register("getShopTime", (time: String) => getShopTime(time))
    sqlContext.udf.register("dealPos", (res: String) => dealPos(res))

    val dir = new File("/tmp/csv_data")
    val children = dir.listFiles.filter(_.isFile)

    children.foreach(f => {
      val name = f.getName.replaceAll(".csv", "")

      println(f.getAbsolutePath + "========>" + name)

      val df = sqlContext.csvFile(f.getAbsolutePath, true, ',', '"')
        .drop("SHOP_VALID_ST")
        .drop("MCHNT_ST")

      val fields = df.schema.fieldNames
        .map(x => {
          x match {
            case "MCHNT_NM" => "unionValue(MCHNT_NM,NEW_NAME) MCHNT_NM"
            case "MCHNT_PHONE" => "unionValue(MCHNT_PHONE,NEW_PHONE) MCHNT_PHONE"
            case "BUSS_HOUR" => "getShopTime(unionValue(BUSS_HOUR,NEW_BUSS_HOUR)) BUSS_HOUR"
            case "flashPay" => "parkInfo(flashPay,free,wifi,park,card) PARK_INF"
            case "sim2" => "getShopValidSt(NEW_NAME,sim2) MCHNT_ST"
            case "MCHNT_LONGITUDE" => "dealPos(MCHNT_LONGITUDE) MCHNT_LONGITUDE"
            case "MCHNT_LATITUDE" => "dealPos(MCHNT_LATITUDE) MCHNT_LATITUDE"
            case "MCHNT_LONGITUDE_WEB" => "dealPos(MCHNT_LONGITUDE_WEB) MCHNT_LONGITUDE_WEB"
            case "MCHNT_LATITUDE_WEB" => "dealPos(MCHNT_LATITUDE_WEB) MCHNT_LATITUDE_WEB"
            case "AMAP_LONGITUDE" => "dealPos(AMAP_LONGITUDE) AMAP_LONGITUDE"
            case "AMAP_LATITUDE" => "dealPos(AMAP_LATITUDE) AMAP_LATITUDE"
            case _ => s"coalesce(${x},'') ${x}"
          }
        })

      val finalDF = df.selectExpr(fields: _*)

      val res = finalDF
        .drop("free")
        .drop("wifi")
        .drop("park")
        .drop("card")
        .drop("NEW_NAME")
        .drop("NEW_PHONE")
        .drop("NEW_BUSS_HOUR")
        .drop("brandName")
        .drop("NEW_Brand")
        .drop("simlarity")

      println(res.count())
      JdbcUtil.save2Mysql("tbl_chmgm_preferential_mchnt_inf_insert", rootNode = "source")(res)
      println(s"insert ${f.getAbsolutePath} is Done!!!")
    })

    sc.stop()
  }

  def getShopValidSt(shopName: String, res: String): Int = {
    shopName match {
      case "2" => 0
      case null => 0
      case _ => {
        res match {
          case "0.0" => 0
          case _ => 2
        }
      }
    }
  }


  def dealPos(res: String): String = {
    res match {
      case "0E-12" => "0"
      case _ => res
    }
  }


  def unionValue(value: String, new_value: String): String = {
    new_value match {
      case null => value
      case "" => value
      case "2" => value
      case _ => new_value
    }
  }

  def getShopTime(oHour: String) = {

    implicit val timeOrdering = new Ordering[String] {
      val convert = (x: String) => if (x.split(":").head.size < 2) s"0${x}" else x

      override def compare(x: String, y: String): Int = {
        convert(x).compare(convert(y))
      }
    }

    val name = oHour.replaceAll(" ", "").replaceAll("：", ":")
    val finalName = if (name == "24" || name == "二十四小时" || name.contains("24小时")) "00:00-23:59"
    else {
      val timeStr = timeRegex.findAllIn(name).toList
      if (timeStr.isEmpty) {
        val dianStr = dianRegex.findAllIn(name).toList
        if (dianStr.isEmpty || dianStr.size == 1) name
        else {
          dianStr.take(2).map(_.replaceAll("点", "")) match {
            case Nil => name
            case head :: second :: Nil => s"$head:00-${if (second.toInt > 10) second else second.toInt + 12}:00"
          }
        }
      }
      else {
        val mm = timeStr.sorted
        val min = mm.min
        val max = mm.max
        s"$min-$max"
      }
    }
    if (chRegex.findFirstIn(finalName).isDefined) "具体以店内公布为准" else finalName
  }

}
