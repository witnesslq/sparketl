package com.unionpay.test.ls

import java.io.PrintWriter

import com.unionpay.db.jdbc.{JdbcSaveMode, JdbcUtil}
import com.unionpay.db.jdbc.JdbcUtil._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/11/28.
  */
object ShopCouponRelation {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("生成shopCouponSql语句")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "4")
    import sqlContext.implicits._

    val relationDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\sl\\tbl_content_relate_shop_coupon")
      .selectExpr("shop_no", "coupon_no")

    val shopDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\sl\\tbl_content_shop_inf")
      .selectExpr("shop_no", "src_shop_no")

    val onlineShopDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\sl\\tbl_content_online_shop")
      .selectExpr("shop_no", "src_shop_no")

    val tmpDF = shopDF.unionAll(onlineShopDF)
    JdbcUtil.save2Mysql("tmp_check")(tmpDF)

    val shopB = sc.broadcast(shopDF.map(r => (r.getAs[String]("src_shop_no"), r.getAs[String]("shop_no"))).collect().toMap)
    val oshopB = sc.broadcast(onlineShopDF.map(r => (r.getAs[String]("src_shop_no"), r.getAs[String]("shop_no"))).collect().toMap)


    val graphDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\result_sim_graph")

    val ds = graphDF.mapPartitions(it => {
      val s = shopB.value
      val o = oshopB.value
      val xTP = it.map(r => (r.getAs[String]("src_shop_no"), r.getAs[Seq[String]]("b_list")))
      val sql = xTP.map { case (sno, lst) =>
        if (s.keySet.contains(sno)) {
          s.get(sno) match {
            case None => Seq("")
            case Some(j) => {
              val xL = lst.map(s.getOrElse(_, "")).filterNot(_.isEmpty)
              //              xL.map(x => s"update tbl_content_relate_shop_coupon set shop_no='${j.trim}' where shop_no='${x.trim}' ;")
              xL.map(x => s"${x.trim},${j.trim}")
            }
          }
        }
        else {
          //          if (o.keySet.contains(sno)) {
          //            o.get(sno) match {
          //              case None => Seq("")
          //              case Some(j) => {
          //                val xL = lst.map(o.getOrElse(_, "")).filterNot(_.isEmpty)
          //                //                xL.map(x => s"update tbl_content_relate_goods_onlineshop set shop_no='${j.trim}' where shop_no='${x.trim}' ;")
          //                xL.map(x => s"${x.trim},${j.trim}")
          //              }
          //            }
          //          } else
          Seq("")

        }
      }
      sql.flatten.filterNot(_.isEmpty)
    })
      .toDF("sql")
      .map(_.getAs[String]("sql")).collect()
    writeSql("relation_shop", ds)


    def writeSql(name: String, ds: Seq[String]) = {
      val pw = new PrintWriter(s"C:\\Users\\ls\\Desktop\\Desktop\\$name.sql")
      ds.foreach(pw.println)
      pw.close()
    }

    sc.stop()
  }

}
