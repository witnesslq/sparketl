package com.unionpay.test.ls

import com.unionpay.etl._
import com.unionpay._
import com.unionpay.db.jdbc.JdbcUtil._
import com.unionpay.db.jdbc.{JdbcSaveMode, JdbcUtil}
import com.unionpay.util.{ConfigUtil, IdGenerator}
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SQLContext, UserDefinedFunction}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sl on 2016/7/7.
  */
object NewBrandInfoJob {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("BrandInfoJob--品牌信息自有任务")
      //      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.yarn.driver.memoryOverhead", "1024")
      .set("spark.yarn.executor.memoryOverhead", "2000")
      .set("spark.network.timeout", "300s")
      .set("spark.executor.heartbeatInterval", "30s")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", spark_shuffle_partitions)
    etl(args)
    sc.stop()
  }

  def etl(args: Array[String])(implicit sqlContext: SQLContext) = {

    val brandInfoDF = fetchUnionBrand
    etlUnion(brandInfoDF)
  }

  def fetchUnionBrand(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val brandInfoTb = "tbl_chmgm_brand_tmp_bak20161128"
    val brandLogic = "tbl_brand_etl_logic_bak20161128"
    val brandDF = JdbcUtil.mysqlJdbcDF(brandInfoTb, "sink")
    val logicDF = JdbcUtil.mysqlJdbcDF(brandLogic, "sink")
    val df = brandDF.as('a)
      .join(logicDF.as('b), $"a.BRAND_NM" === $"b.brand_new_name", "left_outer")
      .selectExpr("b.brand_no old_brand_no", "a.*")
    df
  }


  def etlUnion(brandInfoDF: DataFrame)(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    val getBrandNo = udf(() => IdGenerator.generateBrandID)
    val brandImgTb = "TBL_CHMGM_BRAND_PIC_INF"
    val config = readConfig.union
    val tmpBrandInfoDF = brandInfoDF
      .reNameColumn(config.transForm)
      .appendDefaultVColumn(config.default)
      .addAlwaysColumn
    val brandImgDF = JdbcUtil.mysqlJdbcDF(brandImgTb)
      .filter($"CLIENT_TP" === lit("2"))
      .withColumnRenamed("BRAND_ID", "BRAND_NO")

    save2Mysql(tmpBrandInfoDF, brandImgDF, getBrandNo)
  }

  def fetchBrandImg(brandInfoDF: DataFrame, brandImgDF: DataFrame)(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val imgDF = brandImgDF.filter("BRAND_PIC_PRIO=11 or BRAND_PIC_PRIO=12").as('a)
      .join(brandInfoDF.as('b), $"a.BRAND_NO" === $"b.BRAND_NO", "leftsemi")
      .selectExpr("concat('https://youhui.95516.com/app/image/brand/',SEQ_ID,'.jpg') as BRAND_IMG", "a.BRAND_NO SRC_BRAND_NO", "a.BRAND_PIC_PRIO")
      .select($"SRC_BRAND_NO", concat_ws("@", $"BRAND_PIC_PRIO", $"BRAND_IMG").as("TY_IMG"))
      .groupBy($"SRC_BRAND_NO").agg(concat_ws("|", collect_set($"TY_IMG")).as("MERGE_IMG_LIST"))
      .mapPartitions(it => {
        it.map(row => {
          val srcBrandNo = row.getAs[Long]("SRC_BRAND_NO")
          val imgStrList = row.getAs[String]("MERGE_IMG_LIST")
          val (smallImg, bigImg) = if (imgStrList.isEmpty) ("", "")
          else {
            val tPairs = imgStrList.split("\\|")
            tPairs.size match {
              case 0 => ("", "")
              case 1 => {
                val item = tPairs.head.split("@")
                item.head match {
                  case "11" => ("", item.tail.head)
                  case "12" => (item.tail.head, "")
                  case _ => ("", "")
                }
              }
              case s: Int if s >= 2 => {
                val tMap = tPairs.take(2).map(x => {
                  val ps = x.split("@")
                  (ps.head, ps.tail.headOption getOrElse "")
                }).toMap
                (tMap.getOrElse("12", ""), tMap.getOrElse("11", ""))
              }
            }

          }
          (srcBrandNo, smallImg, bigImg)
        })
      }).toDF("SRC_BRAND_NO", "BRAND_IMG", "BRAND_IMG_DESC")
    imgDF
  }

  def readConfig: BrandInfoConfig = {
    import net.ceedubs.ficus.Ficus._
    val configName = "brandInfo"
    ConfigUtil.readClassPathConfig[BrandInfoConfig](configName)
  }

  def save2Mysql(brandInfoDF: DataFrame, brandImgDF: DataFrame, getBrandNo: UserDefinedFunction)(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    //    val imgDF = fetchBrandImg(brandInfoDF, brandImgDF)
    val finalBrandDF = brandInfoDF.as('a)
      //      .join(imgDF.as('b), $"a.BRAND_NO" === $"b.SRC_BRAND_NO", "left_outer")
      .select(getBrandNo().cast(StringType).as("brand_no"), $"a.BRAND_NO".cast(StringType).as("SRC_BRAND_NO"), trim($"a.BRAND_NM").as("BRAND_NM"), trim($"a.BRAND_INTRO").as("BRAND_INTRO"),
      when(length(trim($"a.BRAND_DESC")) > 1024, substring(trim($"a.BRAND_DESC"), 0, 1024))
        .otherwise(trim($"a.BRAND_DESC")).as("BRAND_DESC"), $"a.ROW_CRT_USR", $"a.ROW_CRT_TS", $"a.TRANS_UPD_TS",
      $"a.REC_UPD_USR", $"a.REC_UPD_TS")
      .withColumn("SRC_BRAND_TP", lit("1"))
      .filter("trim(BRAND_NM)!=''")


    saveToMysql("tbl_content_brand_inf_bak20181128", JdbcSaveMode.Upsert)(finalBrandDF)
  }

}


