package com.unionpay.test.online

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shilong on 2016/11/24.
  */
object ShopSimAll {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("ShopSimAll")
      .setMaster("local[*]")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")

    val dfSim = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\11.29\\result")

    val tmpDF = dfSim.selectExpr(
      "商户代码MCHNT_CD MCHNT_CD", "商户名称MCHNT_NM MCHNT_NM", "商户名称 New_MCHNT_NM", "商户营业地址MCHNT_ADDR MCHNT_ADDR", "商户营业地址 New_MCHNT_ADDR",
      "商户电话MCHNT_PHONE MCHNT_PHONE", "商户电话 New_MCHNT_PHONE", "商户网址MCHNT_URL MCHNT_URL", "商户所属市代码MCHNT_CITY_CD MCHNT_CITY_CD",
      "商户所属市代码 New_MCHNT_CITY_CD", "商户所属区县代码MCHNT_COUNTY_CD MCHNT_COUNTY_CD", "商户所属区县代码 New_MCHNT_COUNTY_CD",
      "商户所在省MCHNT_PROV MCHNT_PROV", "商户所在省 New_MCHNT_PROV", "商圈代码umBUSS_DIST_CD BUSS_DIST_CD", "商户类型IDMCHNT_TYPE_ID MCHNT_TYPE_ID",
      "菜系IDCOOKING_STYLE_ID COOKING_STYLE_ID", "返利比例REBATE_RAT REBATE_RATE", "折扣比例DISCOUNT_RATE DISCOUNT_RATE", "人均消费AVG_CONSUME AVG_CONSUME",
      "积分商户标志POINT_MCHNT_IN POINT_MCHNT_IN", "折扣商户标志DISCOUNT_MCHNT_IN DISCOUNT_MCHNT_IN", "特惠商户标志PREFERENTIAL_MCHNT_I PREFERENTIAL_MCHNT_IN",
      "排序号OPT_SORT_SEQ OPT_SORT_SEQ", "关键字KEYWORDS KEYWORDS", "商户介绍chMCHNT_DESC MCHNT_DESC", "记录创建时间REC_CRT_TS REC_CRT_TS",
      "记录修改时间REC_UPD_TS REC_UPD_TS", "加密位置信息ENCR_LOC_INF ENCR_LOC_INF", "点评次数COMMENT_NUM COMMENT_NUM", "收藏次数FAVOR_NUM FAVOR_NUM", "分享次数SHARE_NUM SHARE_NUM",
      "店铺服务PARK_INF PARK_INF", "营业时间chBUSS_HOUR BUSS_HOUR", "营业时间 New_BUSS_HOUR", "交通信息TRAFFIC_INF TRAFFIC_INF", "招牌服务FAMOUS_SERVICE FAMOUS_SERVICE",
      "点评值COMMENT_VALUE COMMENT_VALUE", "内容IDCONTENT_ID CONTENT_ID", "商户状态MCHNT_ST MCHNT_ST", "商户一级参数MCHNT_FIRST_PAR MCHNT_FIRST_PARA",
      "商户一级参数 new_MCHNT_FIRST_PARA", "商户二级参数MCHNT_SECOND_PARA MCHNT_SECOND_PARA", "商户二级参数 New_MCHNT_SECOND_PARA", "商户经度MCHNT_LONGITUDE MCHNT_LONGITUDE",
      "商户纬度MCHNT_LATITUDE MCHNT_LATITUDE", "商户经度web用MCHNT_LONGITUDE_WEB MCHNT_LONGITUDE_WEB", "商户纬度web用MCHNT_LATITUDE_WEB MCHNT_LATITUDE_WEB",
      "所属银联分公司代码CUP_BRANCH_INS_ID_CD CUP_BRANCH_INS_ID_CD", "商户分店BRANCH_NM BRANCH_NM", "品牌IDBRAND_ID BRAND_ID", "业务位图BUSS_BMP BUSS_BMP",
      "是否通过终端区分门店类型TERM_DIFF_STORE_TP_IN TERM_DIFF_STORE_TP_IN", "记录编码bigintREC_ID REC_ID", "高德商户经度AMAP_LONGITUDE AMAP_LONGITUDE",
      "高德商户纬度AMAP_LATITUDE AMAP_LATITUDE", "云闪付flashPay flashPay", "免密免签free free", "WIFI支持wifi wifi", "停车park park", "刷卡card card",
      "品牌名BRAND_NM brandName", "品牌名 NEW_Brand", "相似similarityIds similarityIds", "相似 simi", "相似门店 simId")

    tmpDF.repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ls\\Desktop\\11.29\\result_tag")

    sc.stop()


  }

}
