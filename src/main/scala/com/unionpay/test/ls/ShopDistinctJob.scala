package com.unionpay.test.ls

import java.util

import com.mongodb.{BasicDBList, BasicDBObject}
import com.unionpay.db.jdbc.JdbcUtil._
import com.unionpay.db.mongo.MongoUtil._
import com.unionpay.util.MathUtil._
import org.apache.spark.mllib.linalg.{Vector, Vectors, DenseVector => DV}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.graphframes.GraphFrame

import scala.collection.JavaConversions._

/**
  * Created by ywp on 2016/8/24.
  */
object ShopDistinctJob {

  private lazy val shopTmpModelPath = "C:\\Users\\ls\\Desktop\\sim\\model\\shopModel"
  private lazy val unionsimilarityPath = "C:\\Users\\ls\\Desktop\\sim\\shop\\similarity"
  private lazy val uniondistinctshopPath = "C:\\Users\\ls\\Desktop\\sim\\shop\\distinctshop"
  private lazy val unioncrawlsimilarityPath = "C:\\Users\\ls\\Desktop\\sim\\shop\\unioncrawlsimilarity"
  private lazy val crawShop = "crawlShopGeo"

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("ShopDistinctJob--商户去重")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.yarn.driver.memoryOverhead", "1024")
      .set("spark.yarn.executor.memoryOverhead", "2000")
      .set("spark.network.timeout", "300s")
      .set("spark.executor.heartbeatInterval", "30s")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")
    //vector转化为 Seq[Double]
    sqlContext.udf.register("vec2Array", (v1: Vector) => v1.toDense.values)
    cleanShop
    sc.stop()
  }


  def cleanShop(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    val oldShopDF = mysqlJdbcDF("tbl_chmgm_preferential_mchnt_inf")
//      .filter($"MCHNT_PROV".isin(Seq("310000", "440000"): _*))
      .selectExpr("trim(MCHNT_CD) MCHNT_CD")
    val unionShopDF = sqlContext.read.parquet(shopTmpModelPath).as('a)
      .join(oldShopDF.as('b), $"a.shopNo" === $"b.MCHNT_CD", "leftsemi")
    val remainUnionShopDF = cleanUnionShop(unionShopDF)
    cleanWithCrawlShop(remainUnionShopDF)
    //todo 银联店铺去重后保存 hdfs 与互联网数据去重相似标准需要先确定
    //remainUnionShopDF.selectExpr("shopNo").write.parquet(uniondistinctshopPath)
  }


  def cleanUnionShop(unionShopDF: DataFrame)(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val seq = Seq("专享", "银联", "券", "观影", "银行", "邮储", "IC卡", "信用卡", "优惠", "6+2", "权益", "活动", "测试", "扫码", "云闪付", "积分", "信用卡", "重阳", "62", "六二", "悦享", "测试", "一元风暴", "约惠星期六")
    val broadcastStopWords = sqlContext.sparkContext.broadcast(seq)
    val shopBrandNames = unionShopDF.selectExpr("shopNo", "brandName").map(r => (r.getAs[String]("shopNo"), r.getAs[String]("brandName"))).collect().toMap
    val broadcastBrandNames = sqlContext.sparkContext.broadcast(shopBrandNames)

    val filterShopUDF = udf((shopIdList: Seq[String]) => {
      val sp = broadcastStopWords.value
      val rex =s"""[${sp.mkString("")}]""".r
      val shopBrands = broadcastBrandNames.value
      Option(shopIdList) match {
        case None => Seq.empty[String]
        case Some(ids) => {
          ids.size match {
            case s: Int if s <= 1 => Seq.empty[String]
            case s: Int if s >= 2 => {
              val mapData = ids.map(x => (x.trim, shopBrands.getOrElse(x.trim, null))).map(x => {
                Option(x._2) match {
                  case None => (x._1, 0)
                  case Some(xx) => if (rex.findFirstIn(xx).isDefined) (x._1, -1) else (x._1, 1)
                }
              })
              val res = mapData.filterNot(_._2 == 0)
              val illegal = res.count(_._2 == -1)
              val normal = res.count(_._2 == 1)
              if (illegal == ids.size || normal == ids.size) ids.drop(1)
              else {
                val x = res.find(_._2 == 1).map(_._1).getOrElse("")
                res.filterNot(_._1 == x).map(_._1)
              }
            }
          }
        }
      }
    })

    val simDF = sqlContext.read.parquet(unionsimilarityPath).selectExpr("shopNo", "otherNo")
    val v = simDF.selectExpr("shopNo id").unionAll(simDF.selectExpr("otherNo id")).distinct().orderBy("id")
    val e = simDF.selectExpr("shopNo src", "otherNo dst").withColumn("relationship", lit("sim"))
    val g = GraphFrame(v, e)
    val result = g.connectedComponents.run()
    val dirtyShopList = result.groupBy("component")
      .agg(filterShopUDF(collect_set("id")).as("dirtyIds"))
      .filter("size(dirtyIds)>1")
      .selectExpr("explode(dirtyIds) dirtyId")
      .map(_.getAs[String]("dirtyId"))
      .collect()
    //去掉重复的shopId
    unionShopDF.filter(!$"shopNo".isin(dirtyShopList: _*))
  }


  def cleanWithCrawlShop(remainUnionShopDF: DataFrame)(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._

    //todo 过滤经纬度非法的店铺
    val df = remainUnionShopDF.filter("shopLng>1")

    val simDF = df.mapPartitions(it => {
      val col = C(crawShop)
      //1.5公里
      it.map(r => {
        val myLocation = new BasicDBList()
        val shopNo = r.getAs[String]("shopNo")
        val shopName1 = r.getAs[String]("shopName")
        val brandName = r.getAs[String]("brandName")
        val spName = r.getAs[Seq[String]]("name")
        val vec1 = r.getAs[DV]("normName")
        val lng = r.getAs[Double]("shopLng")
        val lat = r.getAs[Double]("shopLat")
        myLocation.put(0, lng)
        myLocation.put(1, lat)
        try {
          //shard集群下 near必须用geoNear代替
          val geoNear = new BasicDBObject("$geoNear",
            new BasicDBObject("near", myLocation).append("limit", 100).append("distanceField", "location").append("maxDistance", 1500 / 6378137))
          val shopList = col.aggregate(util.Arrays.asList(geoNear))
            //            .useCursor(true)
            .iterator()
            .map(doc => {
              val vs = doc.get[util.List[Double]]("normName", classOf[util.List[Double]])
              val shopId = doc.getString("shopId")
              val shopName = doc.getString("shopName")
              val spName = doc.get[util.List[String]]("spName", classOf[util.List[String]]).toIndexedSeq
              (shopId, Vectors.dense(vs.map(_.doubleValue()).toArray).toSparse, shopName, spName)
            }).toStream

          val sim = for (
            (shopId, vec2, shopName, spName) <- shopList
          ) yield (shopId, calculateSimilarity(vec1.toSparse, vec2), shopName, spName, calculateSimilarity2(shopName1, shopName))
          (shopNo, brandName, shopName1, spName, sim.filter(_._2 >= 0.4))
        } catch {
          case e: Exception => throw e
        }
      })
    })
      .toDF("unionShopNo", "unionBrandName", "unionShopName", "unionSpName", "crawlShopList")
      .filter("size(crawlShopList)>0")
      .selectExpr("unionShopNo", "unionBrandName", "unionShopName", "unionSpName", "explode(crawlShopList) crawlShopList")
      //      .filter("crawlShopList._1!=''")
      .selectExpr("crawlShopList._2 sim", "crawlShopList._5 rate", "unionShopNo", "unionBrandName", "unionShopName", "unionSpName",
      "crawlShopList._1 crawlShopNo", "crawlShopList._3 crawlShopName", "crawlShopList._4 crawlSpName")
      //todo 暂定相似度界限为0.95
      .filter(!$"sim".isNaN)
      .filter($"sim" >= 0.4)

    simDF.write.mode(SaveMode.Overwrite).parquet(unioncrawlsimilarityPath)

  }

}
