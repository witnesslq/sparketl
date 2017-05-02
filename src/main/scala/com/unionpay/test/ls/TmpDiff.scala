package com.unionpay.test.ls

import java.io.PrintWriter

import com.unionpay.db.jdbc.JdbcUtil._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
  * Created by ls on 2016/9/28.
  */
object TmpDiff {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("CompanySplit---数据分割")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.yarn.driver.memoryOverhead", "1024")
      .set("spark.yarn.executor.memoryOverhead", "2000")
      .set("spark.network.timeout", "300s")
      //todo 云主机 经常网络超时
      .set("spark.executor.heartbeatInterval", "30s")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    implicit val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")

    sqlContext.udf.register("arr2Str", (ids: Seq[String]) => ids.mkString("[", ",", "]"))

    val pw = new PrintWriter("C:\\Users\\ls\\Desktop\\category\\ids.csv")
    //
    //    JdbcUtil
    //      .mysqlJdbcDF("TBL_TMP3", "sink")
    //      .groupBy("THIRD_PARTY_INS_ID")
    //      .agg(collect_set("ENTRY_INS_ID_CD").as("ids"))
    //      .selectExpr("THIRD_PARTY_INS_ID", "ids")
    //      .filter(size($"ids") > 1)
    //      .selectExpr("THIRD_PARTY_INS_ID", "arr2Str(ids) ids")
    //      .selectExpr("trim(THIRD_PARTY_INS_ID) THIRD_PARTY_INS_ID", "ids ENTRY_INS_ID_CD")
    //      .map(_.mkString(""))
    //      .collect()
    //      .foreach(pw.println(_))

    //
    //    val df2 = JdbcUtil
    //      .mysqlJdbcDF("TBL_TMP3", "sink")
    //      .groupBy("THIRD_PARTY_INS_ID")
    //      .agg(collect_set("ENTRY_INS_ID_CD").as("ids"))
    //      .selectExpr("THIRD_PARTY_INS_ID", "ids")
    //      .filter(size($"ids") === 1)
    //      .selectExpr("THIRD_PARTY_INS_ID", "arr2Str(ids) ids")
    //      .selectExpr("trim(THIRD_PARTY_INS_ID) THIRD_PARTY_INS_ID", "trim(ids) ENTRY_INS_ID_CD")

    //    val df = df1.unionAll(df2)


    //    JdbcUtil.save2Mysql("tbl_tmp_mchnt_cd")(df1)

    val df1 = mysqlJdbcDF("TBL_CHMGM_STORE_TERM_RELATION")
      .selectExpr("THIRD_PARTY_INS_ID", "MCHNT_CD")

    val df2 = mysqlJdbcDF("TBL_CHMGM_ACCESS_BAS_INF")
      .selectExpr("ENTRY_INS_ID_CD", "CH_INS_ID_CD", "CH_INS_TP")

    df2.as('a)
      .join(df1.as('b), $"a.CH_INS_ID_CD" === $"b.MCHNT_CD", "left_outer")
      .selectExpr("trim(a.CH_INS_ID_CD) CH_INS_ID_CD", "trim(b.THIRD_PARTY_INS_ID) THIRD_PARTY_INS_ID", "a.CH_INS_TP CH_INS_TP")
      .filter($"CH_INS_TP" === "T")
      .map(_.mkString("[", ",", "]"))
      .collect()
      .foreach(pw.println(_))

    pw.close()

    sc.stop()
  }
}
