package com.unionpay.test.ls

import com.unionpay.db.jdbc.JdbcUtil
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.graphframes.GraphFrame


/**
  * Created by ls on 2016/9/8.
  */
object CompanySplit {

  //"云闪付;免密免签;WIFI支持;停车;刷卡;"

  val str = "商户代码(MCHNT_CD)\t" + "商户名称(MCHNT_NM)\t" + "商户营业地址(MCHNT_ADDR)\t" + "商户电话(MCHNT_PHONE)\t" +
    "商户网址(MCHNT_URL)\t" + "商户所属市代码(MCHNT_CITY_CD)\t" + "商户所属区县代码(MCHNT_COUNTY_CD)\t" + "商户所在省(MCHNT_PROV)\t" +
    "商圈代码(um)(BUSS_DIST_CD)\t" + "商户类型ID(MCHNT_TYPE_ID)\t" + "菜系ID(COOKING_STYLE_ID)\t" + "返利比例(REBATE_RAT)\t" +
    "折扣比例(DISCOUNT_RATE)\t" + "人均消费(AVG_CONSUME)\t" + "积分商户标志(POINT_MCHNT_IN)\t" + "折扣商户标志(DISCOUNT_MCHNT_IN)\t" +
    "特惠商户标志(PREFERENTIAL_MCHNT_I)\t" + "排序号(OPT_SORT_SEQ )\t" + "关键字(KEYWORDS)\t" + "商户介绍(ch)(MCHNT_DESC)\t" +
    "记录创建时间(REC_CRT_TS)\t" + "记录修改时间(REC_UPD_TS)\t" + "加密位置信息(ENCR_LOC_INF)\t" + "点评次数(COMMENT_NUM)\t" +
    "收藏次数(FAVOR_NUM)\t" + "分享次数(SHARE_NUM)\t" + "营业时间(ch)(BUSS_HOUR)\t" +
    "交通信息(TRAFFIC_INF)\t" + "招牌服务(FAMOUS_SERVICE)\t" + "点评值(COMMENT_VALUE)\t" + "内容ID(CONTENT_ID)\t" +
    "商户状态(MCHNT_ST)\t" + "商户一级参数(MCHNT_FIRST_PAR)\t" + "商户二级参数(MCHNT_SECOND_PARA)\t" +
    "商户经度(MCHNT_LONGITUDE)\t" + "商户纬度(MCHNT_LATITUDE)\t" + "商户经度(web用)(MCHNT_LONGITUDE_WEB)\t" +
    "商户纬度(web用)(MCHNT_LATITUDE_WEB)\t" + "所属银联分公司代码(CUP_BRANCH_INS_ID_CD)\t" +
    "商户分店(BRANCH_NM)\t" + "品牌ID(BRAND_ID)\t" + "业务位图(BUSS_BMP)\t" +
    "是否通过终端区分门店类型(TERM_DIFF_STORE_TP_IN)\t" + "记录编码(bigint)(REC_ID)\t" +
    "高德商户经度(AMAP_LONGITUDE)\t" + "高德商户纬度(AMAP_LATITUDE)\t" + "云闪付(flashPay)\t" + "免密免签(free)\t" + "WIFI支持(wifi)\t" + "停车(park)\t" +
    "刷卡(card)\t" + "品牌名(BAND_NM)\t" + "相似(similarityIds)"

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
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")
    val brandDF = fetchUnionBand
    val shopDF = fetchUnionShop
    etl(shopDF, brandDF)
    sc.stop()
  }

  def fetchUnionBand(implicit sqlContext: SQLContext): DataFrame = {
    JdbcUtil.mysqlJdbcDF("tbl_chmgm_brand_inf").selectExpr("BRAND_ID", "BRAND_NM")
  }


  def splitService(inf: String): Seq[String] = {
    val a = inf.charAt(0).toString
    val b = inf.charAt(1).toString
    val c = inf.charAt(2).toString
    val d = inf.charAt(3).toString
    val e = inf.charAt(4).toString
    Seq(a, b, c, d, e)
  }


  def etl(shopDF: DataFrame, brandDF: DataFrame)(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._

    sqlContext.udf.register("arr2Str", (ids: Seq[String]) => ids.mkString("[", "|", "]"))
    sqlContext.udf.register("splitService", (inf: String) => splitService(inf))

    //    val companyDF = fetchUnionCompany
    val entryDF = JdbcUtil.mysqlJdbcDF("TBL_ALL_TMP", "sink")
      .selectExpr("trim(THIRD_PARTY_INS_ID) THIRD_PARTY_INS_ID", "trim(ENTRY_INS_ID_CD) ENTRY_INS_ID_CD")

    val similarityDF = getSimilarityDF
      .selectExpr("arr2Str(dirtyIds) dirtyIds")

    val serviceDF = JdbcUtil.mysqlJdbcDF("tbl_chmgm_shop_inf")
      .selectExpr("MCHNT_CD", "splitService(PARK_INF) service")
      .selectExpr("MCHNT_CD", "service[0] flashPay", "service[1] free", "service[2] wifi", "service[3] park", "service[4] card")


    val tmpDF = shopDF.drop("PARK_INF").as('a)
      .join(serviceDF.as('e), $"a.MCHNT_CD" === $"e.MCHNT_CD")
      .join(brandDF.as('d), $"a.BRAND_ID" === $"d.BRAND_ID", "left_outer")
      .join(entryDF.as('b), $"a.MCHNT_CD" === $"b.THIRD_PARTY_INS_ID", "left_outer")
      .join(similarityDF.as('c), $"c.dirtyIds".contains($"a.MCHNT_CD"), "left_outer")
      .selectExpr("a.*", "e.flashPay", "e.free", "e.wifi", "e.park", "e.card", "coalesce(d.BRAND_NM,'') BRAND_NM",
        "case when trim(b.ENTRY_INS_ID_CD)='' then 'xxxxxx' else trim(b.ENTRY_INS_ID_CD) end ENTRY_INS_ID_CD",
        "coalesce(regexp_replace(c.dirtyIds,a.MCHNT_CD,''),'[]') dirtyIds")
      .drop("area_nm")


    val fields = tmpDF.schema.fieldNames.filterNot(_ == "ENTRY_INS_ID_CD")

    val df = tmpDF.selectExpr("ENTRY_INS_ID_CD", s"concat_ws('::::',${fields.mkString(",")}) line")
      .groupBy("ENTRY_INS_ID_CD")
      .agg(collect_list("line").as("data"))

    val pp = "C:/Users/ls/Desktop/data2"
    val sb = "C:/Users/ls/Desktop/csv2"

    df.write.mode(SaveMode.Overwrite).partitionBy("ENTRY_INS_ID_CD").parquet(pp)

    //    val groupDF = sqlContext.read.parquet(pp)
    //
    //    //    val tmp = companyDF.selectExpr("sub_company_nm").map(_.getAs[String]("sub_company_nm")).collect()
    //
    //    val tmp = entryDF
    //      .selectExpr("case when trim(ENTRY_INS_ID_CD)='' then 'xxxxxx' else trim(ENTRY_INS_ID_CD) end ENTRY_INS_ID_CD")
    //      .map(_.getAs[String]("ENTRY_INS_ID_CD"))
    //      .collect()
    //      .distinct
    //
    //    //    val companyNames = tmp :+ "xxxxxx"
    //
    //
    //    tmp.map(x => {
    //      val gDF = groupDF.filter(s"ENTRY_INS_ID_CD='${x}'")
    //      val pw = new PrintWriter(s"${sb}/${x}.csv", "UTF-8")
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

  }


  def fetchUnionShop(implicit sqlContext: SQLContext): DataFrame = {
    val shopTb = "tbl_chmgm_shop_inf"
    JdbcUtil.mysqlJdbcDF(shopTb)
  }

  def getSimilarityDF(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val simDF = sqlContext.read.parquet("C:/Users/ls/Desktop/similarity_test")
      .selectExpr("shopNo", "otherNo")

    val v = simDF.selectExpr("shopNo id").unionAll(simDF.selectExpr("otherNo id")).distinct().orderBy($"id")
    val e = simDF.selectExpr("shopNo src", "otherNo dst").withColumn("relationship", lit("sim"))
    val g = GraphFrame(v, e)
    val result = g.connectedComponents.run()

    val tmpDF = result.groupBy("component")
      .agg(collect_set("id").as("dirtyIds"))
      .selectExpr("dirtyIds")

    tmpDF
  }
}