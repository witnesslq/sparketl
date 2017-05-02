package com.unionpay.mllib

import com.unionpay.db.mongo.MongoUtil._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.storage.StorageLevel

/**
  * Created by ywp on 2016/8/22.
  */
object CrawlShopWordsVectorJob {

  private lazy val shopMongoModel = "crawlShopGeo_test"
  private lazy val shopCrawlModel = "/crawl/model/shopModel_test"
  private lazy val shopNamew2c = "/mllib/pipeline/shopnameword2cev"

  def doIt(pipeline: Pipeline)(implicit sqlContext: SQLContext) {
    trainCrawlShopModel(pipeline)
  }


  def trainCrawlShopModel(pipeline: Pipeline)(implicit sqlContext: SQLContext) = {

    import sqlContext.implicits._

    val shopDF = sqlContext
      .fetchCrawlShopDF
      //todo
      .filter($"city".cast(StringType).rlike("""^310.*"""))
      .selectExpr("_id.oid shopId", "shopName", "coordinates location", "sp(shopName,111) spName")
      .persist(StorageLevel.MEMORY_AND_DISK)



    var word2VecModel: PipelineModel = null
    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)
    //先检查 hdfs是否训练好了模型 没有则训练模型
    if (!fs.exists(new Path(shopNamew2c))) {
      word2VecModel = pipeline.fit(shopDF)
      //todo 保存互联网word2Vector模型
      word2VecModel.write.overwrite().save(shopNamew2c)
    } else {
      word2VecModel = PipelineModel.read.load(shopNamew2c)
    }

    val modelDF = word2VecModel.transform(shopDF)
      .repartition(6)
      .selectExpr("shopId", "shopName", "location", "normName", "spName")

    //模型保存至hdfs
    modelDF.write.mode(SaveMode.Overwrite).parquet(shopCrawlModel)


    //todo mongo分片速度太慢 使用原有的mongo店铺数据组合店铺的分词
    //分词向量模型保存至mongo
    modelDF
      .selectExpr("shopId", "vec2Array(normName) normName", "location", "shopName", "spName")
      //shopId 当 mongo _id
      .withColumn("_id", $"shopId")
      .save2Mongo(shopMongoModel)

    //todo run job 之前先删除collection 创建mongo分片
    createOrDefaultShardIndex(shopMongoModel, "shopName")

    //创建2d索引
    createOrDefault2dIndex(shopMongoModel)


  }
}
