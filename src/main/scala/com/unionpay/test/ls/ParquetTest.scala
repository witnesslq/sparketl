package com.unionpay.test.ls

import java.io.PrintWriter

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/11/28.
  */
object ParquetTest {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("生成sql语句")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "4")
    import sqlContext.implicits._
    //    val graphDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\result")

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

    val sn = udf((o: String, n: String, b: String) => {
      val on = o.trim.replaceAll(" ", "")
      val nn = n.trim.replaceAll(" ", "")
      if (nn.isEmpty) {
        if (on.contains(b))
          on
        else
          b + "(" + on + ")"
      } else nn
    })

    val onlineShopIds = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\sl\\tbl_content_online_shop")
      .selectExpr("src_shop_no")
      .map(_.getAs[String]("src_shop_no")).collect()

    val df = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\result_new_1")
      .filter($"shopNo".isin(onlineShopIds: _*))

    /*val fs = df.schema.fieldNames
    val shopNo = fs(0)
    val oldShopName = fs(1)
    val newShopName = fs(2)
    val oldBrandName = fs(3)
    val newBrandName = fs(4)
    val st = fs(5)
    val ex = Seq(s"trim($shopNo) src_shop_no", s"trim($oldShopName) oldShopName", s"delName($newShopName) newShopName",
      s"bn($oldBrandName,$newBrandName) brandName", s"st($st) shop_valid_st")
    val ds = df.selectExpr(ex: _*)
      .select('src_shop_no, sn('oldShopName, 'newShopName, 'brandName).as('shop_nm), 'shop_valid_st)
      .map(r => {
        val s = r.getAs[String]("src_shop_no")
        val n = r.getAs[String]("shop_nm").replace("\'", "\\'")
        val st = r.getAs[String]("shop_valid_st")
        s"update tbl_content_shop_inf set shop_nm='$n',shop_valid_st='$st' where src_shop_no='$s';"
      }).collect()
    def writeSql(name: String, ds: Seq[String]) = {
      val pw = new PrintWriter(s"C:\\Users\\ls\\Desktop\\Desktop\\$name.sql")
      ds.foreach(pw.println)
      pw.close()
    }
    writeSql("shop_again", ds)*/

     val fs = df.schema.fieldNames
     val shopNo = fs(0)
     val oldShopName = fs(1)
     val newShopName = fs(2)
     val oldBrandName = fs(3)
     val newBrandName = fs(4)
     val ex = Seq(s"trim($shopNo) src_shop_no", s"trim($oldShopName) oldShopName", s"delName($newShopName) newShopName",
       s"bn($oldBrandName,$newBrandName) brandName")
     val ds = df.selectExpr(ex: _*)
       .select('src_shop_no, sn('oldShopName, 'newShopName, 'brandName).as('shop_nm))
       .map(r => {
         val s = r.getAs[String]("src_shop_no")
         val n = r.getAs[String]("shop_nm").replace("\'", "\\'")
         s"update tbl_content_shop_inf set shop_nm='$n' where src_shop_no='$s';"
       }).collect()

     ds.foreach(println(_))
     def writeSql(name: String, ds: Seq[String]) = {
       val pw = new PrintWriter(s"C:\\Users\\ls\\Desktop\\Desktop\\$name.sql")
       ds.foreach(pw.println)
       pw.close()
     }
     writeSql("onlineShop", ds)
    sc.stop()
  }

}
