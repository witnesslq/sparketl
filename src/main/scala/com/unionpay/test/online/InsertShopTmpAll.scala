package com.unionpay.test.online

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/10/20.
  */
object InsertShopTmpAll {

  private lazy val timeRegex ="""\d{1,2}(:|：)\d{2}""".r
  private lazy val dianRegex ="""\d{1,2}点""".r
  private lazy val chRegex ="""[\u4e00-\u9fa5]""".r

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("InsertData to CMS === 插入数据")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "4")


    sqlContext.udf.register("getShopTime", (time: String) => getShopTime(time))
    sqlContext.udf.register("delName", (n: String) => {
      val nm = n.trim.replaceAll(" ", "")
      nm match {
        case "2" | "2.0" => ""
        case _ => nm
      }
    })

    sqlContext.udf.register("bn", (o: String, n: String) => {
      val on = o.trim.replaceAll(" ", "")
      val nn = n.trim.replaceAll(" ", "")
      if (nn.isEmpty) on else nn
    })



    sqlContext.udf.register("st", (st: String) => {
      val res = st.trim.replaceAll(" ", "")
      res match {
        case "0" | "0.0" => "0"
        case _ => "1"
      }
    })

    sqlContext.udf.register("parkInfo", (flashPay: String, free: String, wifi: String, park: String, card: String) => "" + flashPay + free + wifi + park + card)
    sqlContext.udf.register("getShop_st", (cardStr: String) => {
      val card = cardStr match {
        case "0.0" | "0" => "0"
        case _ => "1"
      }
      card
    })

    sqlContext.udf.register("sn", (o: String, n: String, b: String) => {
      val on = o.trim.replaceAll(" ", "")
      val nn = n.trim.replaceAll(" ", "")
      if (nn.isEmpty) {
        if (on.contains(b))
          on
        else
          b + "(" + on + ")"
      } else nn
    })

    val shopDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\11.29\\result_tag").drop("PARK_INF")


    val fields = shopDF.schema.fieldNames.map(x => {
      x match {
        case "flashPay" => "parkInfo(st(flashPay),st(free),st(wifi),st(park),st(card)) PARK_INF"
        case "MCHNT_ADDR" => "bn(MCHNT_ADDR,New_MCHNT_ADDR) MCHNT_ADDR"
        case "MCHNT_PHONE" => "bn(MCHNT_PHONE,New_MCHNT_PHONE) MCHNT_PHONE"
        case "MCHNT_CITY_CD" => "bn(MCHNT_CITY_CD,New_MCHNT_CITY_CD) MCHNT_CITY_CD"
        case "MCHNT_COUNTY_CD" => "bn(MCHNT_COUNTY_CD,New_MCHNT_COUNTY_CD) MCHNT_COUNTY_CD"
        case "MCHNT_PROV" => "bn(MCHNT_PROV,New_MCHNT_PROV) MCHNT_PROV"
        case "BUSS_HOUR" => "getShopTime(bn(BUSS_HOUR,New_BUSS_HOUR)) BUSS_HOUR"
        case "MCHNT_FIRST_PARA" => "bn(MCHNT_PROV,new_MCHNT_FIRST_PARA) MCHNT_FIRST_PARA"
        case "MCHNT_SECOND_PARA" => "bn(MCHNT_SECOND_PARA,New_MCHNT_SECOND_PARA) MCHNT_SECOND_PARA"
        case "brandName" => "bn(brandName,NEW_Brand) brandName"
        case "simi" => "getShop_st(simi) SHOP_VALID_ST"
        case "MCHNT_NM" => "sn(MCHNT_NM,New_MCHNT_NM,brandName) MCHNT_NM"
        case _ => s"coalesce(${x},'') ${x}"
      }
    })

    val df = shopDF.selectExpr(fields: _*)
      .drop("free")
      .drop("wifi")
      .drop("park")
      .drop("card")
      .drop("similarityIds")
      .drop("simi")
      .drop("simId")
      .drop("NEW_Brand")
      .drop("New_MCHNT_SECOND_PARA")
      .drop("new_MCHNT_FIRST_PARA")
      .drop("New_BUSS_HOUR")
      .drop("New_MCHNT_PROV")
      .drop("New_MCHNT_COUNTY_CD")
      .drop("New_MCHNT_CITY_CD")
      .drop("New_MCHNT_ADDR")
      .drop("New_MCHNT_PHONE")
      .drop("New_MCHNT_NM")


    df.repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ls\\Desktop\\11.29\\result_db2mysql_1")
    sc.stop()

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

  def getShopValidSt(shopName: String, res: String): Int = {
    shopName match {
      case "2" => 0
      case null => 0
      case _ => {
        res match {
          case "0.0" => 0
          case _ => 1
        }
      }
    }
  }
}
