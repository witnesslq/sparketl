package com.unionpay.test.ls

import com.unionpay.db.jdbc.JdbcUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/10/24.
  */
object WriteToDb {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("DataDistinct === 数据去重")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")

    //    val shopDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\数据\\上线后数据备份\\上线数据\\处理前数据\\exportData\\data1202\\tbl_content_shop_inf30")
    val brandDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\数据\\上线后数据备份\\上线数据\\处理前数据\\exportData\\data1202\\tbl_content_brand_inf30")
      .filter("date_format(row_crt_ts,'yyyyMMdd')>'19900101'")
    //    val couponDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\数据\\上线后数据备份\\上线数据\\处理前数据\\exportData\\data1202\\tbl_content_coupon_inf30")
    //    val shopCouponDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\数据\\上线后数据备份\\上线数据\\处理前数据\\exportData\\data1202\\tbl_content_relate_shop_coupon30")

    //    JdbcUtil.save2Mysql("tbl_content_coupon_inf")(couponDF)
    //    JdbcUtil.save2Mysql("tbl_content_shop_inf")(shopDF)
    //    JdbcUtil.save2Mysql("tbl_content_relate_shop_coupon")(shopCouponDF)
    JdbcUtil.save2Mysql("tbl_content_brand_inf")(brandDF)


    sc.stop()
  }

}
