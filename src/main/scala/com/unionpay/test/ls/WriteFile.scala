package com.unionpay.test.ls

import java.io.PrintWriter

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.unionpay.db.mongo.MongoUtil._


/**
  * Created by ls on 2016/9/8.
  */
object WriteFile {


  val str = "优惠id(_id)\t" + "(银行卡类型)cardTypeList\t" + "(所属优惠大类)categoryList\t" + "(城市编码)city\t" + "(优惠可得性评分)convenienceScore\t" +
    "(记录创建时间)createAt\t" + "(优惠描述)description\t" + "(优惠力度评分)discountScore\t" + "(截止时间)endDate\t" + "(单日最高优惠额度)maxDiscount\t" +
    "(次数限制)maxPurchase\t" + "(最低消费)minConsumption\t" + "(优惠模式枚举)mode\t" + "(参与流程)participationType\t" + "(支付方式)paymentTypeList\t" +
    "(优惠描述图片)pictureList\t" + "(商铺id)shopId\t" + "(原始链接)sourceUrl\t" + "(起始时间)startDate\t" + "(营业状态)status\t" +
    "(优惠描述缩略图)thumbnailImg\t" + "(优惠标题)title\t" + "(记录修改时间)updateAt\t" + "(使用时间说明)validDateDesc\t" + "(星期几限制)validWeekDay\t" +
    "(银行id)coupon_launch_bank_no\t" + "(优惠细则)limitDetails\t" + "(店铺名)shopName"

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("ssssssbbbbbb")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)

    sqlContext.setConf("spark.sql.shuffle.partition", "6")
    sqlContext.udf.register("addStr", (bankNo: String) => "aa" + bankNo)

    val pp = "C:\\Users\\ls\\Desktop\\bank_data"
    val sb = "C:\\Users\\ls\\Desktop\\bank_csv"

    val groupDF = sqlContext.read.parquet(pp)


//    val tmp = sqlContext.fetchCrawlCouponDF
//      .selectExpr("bankCode")
//      .map(_.getAs[Seq[String]]("bankCode"))
//      .map(_.mkString(""))
//      .collect()
//      .distinct
//
//    tmp.map(x => {
//      val gDF = groupDF.filter(s"bankCode='aa${x}'")
//
//      val pw = new PrintWriter(s"${sb}/${x}.csv")
//      pw.println(str)
//      gDF.selectExpr("data").map(row => {
//        val line = row.getAs[Seq[String]]("data")
//        val s = line.map(r => {
//          val t = r.replaceAll("\"", "")
//          t.replaceAll("::::", "\t")
//        })
//        s.mkString("\n")
//      }).collect.foreach(pw.println)
//      pw.close()
//    })

    sc.stop()
  }

}



