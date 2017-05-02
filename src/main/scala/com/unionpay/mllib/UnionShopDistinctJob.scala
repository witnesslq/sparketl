package com.unionpay.mllib

import java.lang.Double
import java.util

import com.mongodb.{BasicDBList, BasicDBObject}
import com.unionpay.db.jdbc.JdbcUtil
import com.unionpay.db.mongo.MongoUtil._
import com.unionpay.util.MathUtil._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.linalg.{Vectors, DenseVector => DV}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._
import scala.sys.process._

/**
  * Created by ywp on 2016/8/22.
  */
object UnionShopDistinctJob {

  private lazy val shopTmpModelPath = "/unionpay/model/shopModel_test"
  private lazy val shopMongoModel = "unionShopGeo_test"
  private lazy val similarityPath = "/unionpay/shop/similarity_test"
  private lazy val shopNamew2c = "/mllib/pipeline/shopnameword2cev"

  def doIt(pipeline: Pipeline)(implicit sqlContext: SQLContext) {


    /*val hadoopConf = sc.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)
    //先检查 hdfs是否训练好了模型 没有则训练模型
    if (!fs.exists(new Path(shopTmpModelPath))) trainUnionShopModel
    //已经计算直接返回
    if (fs.exists(new Path(similarityPath))) {
      return
    }*/

    val (illegalDF, normalDF) = trainUnionShopModel(pipeline: Pipeline)

    //统计银联自有数据的相似度
    statisticsSimilarityShop(illegalDF, normalDF)

  }


  def statisticsSimilarityShop(illegalDF: DataFrame, normalDF: DataFrame)(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._

    //    val shopModelDF = sqlContext.read.parquet(shopTmpModelPath)


    //每一个店铺以及他附近的1000米范围店铺列表
    val normalResult = normalDF.mapPartitions(it => {
      //todo 1、银联店铺经纬度与实际偏差都在1公里左右 2、训练好的shopMode存入mongo 3、mongo负责geo计算
      val col = C(shopMongoModel)
      it.map(row => {
        val myLocation = new BasicDBList()
        val shopNo = row.getAs[String]("shopNo")
        val vec1 = row.getAs[DV]("normName")
        val brandName = row.getAs[String]("brandName")
        val shopName = row.getAs[String]("shopName")
        val spName = row.getAs[Seq[String]]("spName")
        val lng = row.getAs[Double]("shopLng")
        val lat = row.getAs[Double]("shopLat")
        myLocation.put(0, lng)
        myLocation.put(1, lat)
        //附近1公里范围内的shop
        try {
          val geoNear = new BasicDBObject("$geoNear",
            new BasicDBObject("near", new BasicDBObject("type", "Point").append("coordinates", myLocation))
              .append("distanceField", "location").append("spherical", true).append("maxDistance", 1000))
          val shopList = col.aggregate(util.Arrays.asList(geoNear))
            /*val shopList = col.find(new BasicDBObject("location",
              new BasicDBObject("$near",
                new BasicDBObject("$geometry",
                  new BasicDBObject("type", "Point")
                    .append("coordinates", myLocation))
                  .append("$maxDistance", 1000)))
            )
              //mongo游标经常超时
              .noCursorTimeout(true)*/
            .useCursor(true)
            .iterator()
            .map(doc => {
              val vs = doc.get[util.List[Double]]("normName", classOf[util.List[Double]])
              val id = doc.getString("_id")
              val brandName = doc.getString("brandName")
              val shopName = doc.getString("shopName")
              val spName = doc.get[util.List[String]]("spName", classOf[util.List[String]]).toIndexedSeq
              (id, Vectors.dense(vs.map(_.doubleValue()).toArray).toSparse, brandName, shopName, spName)
            })
            .toSeq.par
          //直接从mongo拉取
          val sim = for (
            (id, vec2, brandName, shop, spName) <- shopList if id != shopNo
          ) yield (id, calculateSimilarity(vec1.toSparse, vec2), brandName, shop, spName)
          (shopNo, brandName, shopName, spName, sim.toArray)
        } catch {
          //发生异常清理相似度结果文件
          case e: Exception => s"hdfs dfs -rm -r $similarityPath".!; throw e
        }
      })
    })
      .toDF("shopNo", "brandName", "shopName", "spName", "shopList")
      .filter("size(shopList)>0")
      .selectExpr("shopNo", "brandName", "shopName", "spName", "explode(shopList) shopList")
      .selectExpr("shopList._2 sim", "shopNo", "brandName", "shopName", "spName", "shopList._1 otherNo",
        "shopList._3 otherBrand", "shopList._4 otherShop", "shopList._5 otherName")
      .filter(!$"sim".isNaN)
      .filter("sim=1")
      .mapPartitions(it => {
        it.map(row => {
          val sim = row.getAs[Double]("sim")
          val shopNo = row.getAs[String]("shopNo")
          val brandName = row.getAs[String]("brandName")
          val shopName = row.getAs[String]("shopName")
          val spName = row.getAs[Seq[String]]("spName")
          val otherNo = row.getAs[String]("otherNo")
          val otherBrand = row.getAs[String]("otherBrand")
          val otherShop = row.getAs[String]("otherShop")
          val otherName = row.getAs[Seq[String]]("otherName")
          val union_id = if (shopNo > otherNo) s"${shopNo}_$otherNo" else s"${otherNo}_$shopNo"
          (union_id, Seq((shopNo, brandName, shopName, spName, sim), (otherNo, otherBrand, otherShop, otherName, sim)))
        })
      })
      .reduceByKey {
        case (m1, m2) => m1
      }.toDF("unionId", "seqData")
      .selectExpr("seqData[0] one", "seqData[1] two")
      .selectExpr("one._5 sim", "one._1 shopNo", "one._2 brandName", "one._3 shopName", "one._4 spName",
        "two._1 otherNo", "two._2 otherBrand", "two._3 otherShop", "two._4 otherName")

    val illegalResult = illegalDF.as('a)
      .join(illegalDF.as('b), $"a.shopNo" !== "b.shopNo")
      .selectExpr("similarity(a.normName,b.normName) sim", "a.shopNo", "a.brandName", "a.shopName", "a.spName",
        "b.shopNo otherNo", "b.brandName otherBrand", "b.shopName otherShop", "b.spName otherName")
      .mapPartitions(it => {
        it.map(row => {
          val sim = row.getAs[Double]("sim")
          val shopNo = row.getAs[String]("shopNo")
          val brandName = row.getAs[String]("brandName")
          val shopName = row.getAs[String]("shopName")
          val spName = row.getAs[Seq[String]]("spName")
          val otherNo = row.getAs[String]("otherNo")
          val otherBrand = row.getAs[String]("otherBrand")
          val otherShop = row.getAs[String]("otherShop")
          val otherName = row.getAs[Seq[String]]("otherName")
          val union_id = if (shopNo > otherNo) s"${shopNo}_$otherNo" else s"${otherNo}_$shopNo"
          (union_id, Seq((shopNo, brandName, shopName, spName, sim), (otherNo, otherBrand, otherShop, otherName, sim)))
        })
      }).reduceByKey {
      case (m1, m2) => m1
    }.toDF("unionId", "seqData")
      .selectExpr("seqData[0] one", "seqData[1] two")
      .selectExpr("one._5 sim", "one._1 shopNo", "one._2 brandName", "one._3 shopName", "one._4 spName",
        "two._1 otherNo", "two._2 otherBrand", "two._3 otherShop", "two._4 otherName")
      .filter(!$"sim".isNaN)
      .filter("sim=1")

    val result = illegalResult.unionAll(normalResult).coalesce(6)

    //保存最终相似度结果
    result.write.mode(SaveMode.Overwrite).parquet(similarityPath)

  }

  def trainUnionShopModel(pipeline: Pipeline)(implicit sqlContext: SQLContext): (DataFrame, DataFrame) = {

    import sqlContext.implicits._

    val brandDF = JdbcUtil.mysqlJdbcDF("tbl_chmgm_brand_inf")
      .selectExpr("trim(BRAND_ID) brand_no", "trim(BRAND_NM) brand_nm")

    //去掉验证、测试 商户
    val shopDF = JdbcUtil.mysqlJdbcDF("TBL_CHMGM_PREFERENTIAL_MCHNT_INF")
      //todo
      .filter($"MCHNT_PROV".rlike("""^310.*"""))
      .filter(!$"MCHNT_NM".contains("验证"))
      .filter(!$"MCHNT_NM".contains("测试"))
      .selectExpr("MCHNT_CD shopNo", "regexp_replace(trim(MCHNT_NM), '[;； ]', '') shopName",
        "cast(AMAP_LATITUDE as double) shop_lat", "cast(AMAP_LONGITUDE as double) shop_lng",
        "trim(BRAND_ID) brand_no")

    val tmpShopDF = shopDF.as('a)
      .join(broadcast(brandDF).as('b), $"a.brand_no" === $"b.brand_no", "left_outer")
      .selectExpr("a.shopNo", "sp(unionBS(a.shopName,coalesce(b.brand_nm,'')),a.shop_lat) spName", "shopName", "brand_nm brandName",
        "a.shop_lat shopLat", "a.shop_lng shopLng")
      .persist(StorageLevel.MEMORY_AND_DISK)


    //todo
    val word2VecModel = PipelineModel.read.load(shopNamew2c)

    val modelDF = word2VecModel.transform(tmpShopDF)
      .selectExpr("shopNo", "normName", "shopLat", "shopLng", "shopName", "brandName", "spName")
      .repartition(6)
      .persist(StorageLevel.MEMORY_AND_DISK)

    /* val modelDF = pipeline.fit(tmpShopDF).transform(tmpShopDF)
       .selectExpr("shopNo", "normName", "shopLat", "shopLng", "shopName", "brandName", "spName")
       .repartition(6)
       .persist(StorageLevel.MEMORY_AND_DISK)*/

    //todo 保存模型hdfs
    modelDF.write.mode(SaveMode.Overwrite).parquet(shopTmpModelPath)

    //todo 经纬度为0的不用保存mongo计算
    val normalModeDF = modelDF.filter("shopLat>1")

    //数据保存到mongo利用mongo的geo地理位置计算
    normalModeDF.selectExpr("shopNo", "vec2Array(normName) normName", "shopLat", "shopLng", "shopName", "brandName", "spName")
      .mapPartitions(it => {
        it.map(r => {
          val shopNo = r.getAs[String]("shopNo")
          val normName = r.getAs[Seq[Double]]("normName")

          val shopName = r.getAs[String]("shopName")
          val brandName = r.getAs[String]("brandName")
          val spName = r.getAs[Seq[String]]("spName")

          val lat = r.getAs[Double]("shopLat")
          val lng = r.getAs[Double]("shopLng")
          val location = GeoLocation("Point", Array(lng, lat))
          (shopNo, location, normName, brandName, shopName, spName)
        })
      }).toDF("shopNo", "location", "normName", "brandName", "shopName", "spName")
      //shopNo 当 mongo _id
      .withColumn("_id", $"shopNo")
      .save2Mongo(shopMongoModel)

    //todo 数据较少无需分片
    createOrDefaultShardIndex(shopMongoModel, "shopName")

    //创建2dSphere 索引
    createOrDefault2dSphereIndex(shopMongoModel)

    val illegalDF = modelDF.filter("shopLat<1")

    (illegalDF, normalModeDF)

  }

}

case class GeoLocation(`type`: String, coordinates: Array[Double])