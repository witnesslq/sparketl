package com.unionpay.test

import com.mongodb.BasicDBObject
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import com.unionpay.db.mongo.MongoUtil._

import scala.collection.JavaConversions._
import com.mongodb._
import com.unionpay.util.JsonUtil

/**
  * Created by ywp on 2016/8/19.
  */
object MongoCluster {


  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("MongoCluster")
    //      .setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    val shopMongoModel = "xxx"


    val col = C(shopMongoModel)
    println(JsonUtil.toJacksonPrettyString(col.listIndexes.toSeq.map(_.toJson)))
    println("xxx:" + col.listIndexes.toSeq.find(_.toJson.contains("hashed")).isEmpty)

    //创建mongo分片
    createOrDefaultShardIndex(shopMongoModel)

    //分词向量模型保存至mongo
    val l = Seq(111.0, 22)
    (0.to(10000).map((_, l))).toDF("id", "location")
      .selectExpr("id", "location")
      .save2Mongo(shopMongoModel)

    //创建2d索引
    createOrDefault2dIndex(shopMongoModel)

    sc.stop()
  }

}
