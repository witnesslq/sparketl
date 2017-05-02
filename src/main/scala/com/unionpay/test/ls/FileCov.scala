package com.unionpay.test.ls

import java.io.PrintWriter
import java.math.{BigDecimal => javaBigDecimal}

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import com.unionpay.db.mongo.MongoUtil._

/**
  * Created by ls on 2016/9/2.
  */
object FileCov {

  val serviceMap = Map("免费停车" -> "停车", "无线上网" -> "WIFI支持", "可以刷卡" -> "刷卡")
  val default = "云闪付;免密免签;WIFI支持;停车;刷卡;"
  val ds = default.split(";")
  val x100 = new javaBigDecimal(100)

  val str = "银行卡id(bankCode)\t" + "(商铺名)shopId\t" + "(一级分类)category\t" + "(二级分类)subCategory\t" + "(国家编码)country\t" +
    "(城市编码)city\t" + "(品牌名)brand\t" + "(店铺名)shopName\t" + "(店铺地址)address\t" + "(店铺简介)description\t" +
    "(点评评分)score\t" + "(logo图片地址)logo\t" + "(经度)shop_lnt\t" + "(纬度)shop_lat\t" + "(电话)telephones\t" +
    "(商圈)tradingArea\t" + "(商铺图片)pictureList\t" + "(标签)tags\t" + "(平均消费)averagePrice\t" + "(营业时间)hours\t" +
    "(营业状态)status\t" + "(商铺类型)type\t" + "(记录创建时间)createAt\t" + "(记录修改时间)updateAt"

  def main(args: Array[String]) {
    val conf = new SparkConf()
      //      .setMaster("local[*]")
      .setAppName("getMongoShopData == 抽取银行对应门店信息")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2000m")
      .set("spark.yarn.driver.memoryOverhead", "2048")
      .set("spark.yarn.executor.memoryOverhead", "2000")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "32")
    import sqlContext.implicits._

    val shopIdsDF = sqlContext.fetchCrawlCouponDF
      .selectExpr("bankCode[0] bankCode", "shopIdList")
      .filter($"bankCode" === "030500")
      .selectExpr("bankCode", "explode(shopIdList) shopId")
      .distinct()


    val ids = shopIdsDF.selectExpr("shopId").map(_.getAs[String]("shopId")).collect()

    sqlContext.udf.register("hasServices", (tags: Seq[String]) => shopService(tags))


    val shopDF = sqlContext.fetchCrawlShopDF
      .filter($"_id.oid".isin(ids: _*))
      .selectExpr("_id.oid shopId", "case when size(category)=0 then '' else trim(category[0]) end category",
        "case when size(subCategory)=0 then '' else trim(subCategory[0]) end subCategory", "cast(country as string) country",
        "cast(city as string) city", "trim(brand) brand", "trim(shopName) shopName", "trim(address) address", "trim(description) description",
        "case when score=0 then '' else cast(score as string) end score", "trim(logo) logo",
        "case when size(coordinates)=0 then '' else substring(cast(coordinates[0] as string),0,20) end shop_lnt",
        "case when size(coordinates)<2 then ''   else substring(cast(coordinates[1] as string),0,20) end shop_lat",
        "case when instr(telephones[0],',')>0 then trim(split(telephones[0],',')[0]) when instr(telephones[0],' ')>0  then trim(split(telephones[0],' ')[0])  when size(telephones)=0 then '' else  substring(trim(telephones[0]),0,20) end  shop_contact_phone",
        "trim(tradingArea) tradingArea", "case when instr(pictureList[0],';')>0 then split(pictureList[0],';')[0] when size(pictureList)=0 then ''  else pictureList[0] end  shop_image",
        "case when size(tags)=0 then '00000' else hasServices(tags) end shop_service", "averagePrice",
        "case when length(trim(regexp_replace(hours,'：',':')))>60 then substring(trim(regexp_replace(hours,'：',':')),0,60) else trim(regexp_replace(hours,'：',':')) end busi_time",
        "case when status=0 then '' else cast(status as string) end status", "cast(type as string) type",
        "case when createAt is null then '' when date_format(createAt,'yyyy')<'2000' then '' else date_format(createAt,'yyyyMMdd') end createAt",
        "case when updateAt is null then '' when date_format(updateAt,'yyyy')<'2000' then '' else date_format(updateAt,'yyyyMMdd') end updateAt",
        "trim(landArr) landArr"
      )

    val finalDF = shopDF.as('a)
      .join(broadcast(shopIdsDF.as('b)), $"b.shopId" === $"a.shopId", "left_outer")
      .selectExpr("b.bankCode", "a.*")


    val path = "/tmp/tmp_0305.csv"
    val pw = new PrintWriter(path, "UTF-8")
    pw.println(str)

    finalDF.map(_.mkString("\t")).collect()
      .foreach(pw.println)

    pw.close()

    sc.stop()
  }

  def shopService(tags: Seq[String]) = {
    val hasServices = serviceMap.filterKeys(tags.contains).map(_._2).toSeq
    val result = ds.map(s => {
      if (hasServices.contains(s)) 1
      else 0
    })
    result.mkString("")
  }
}
