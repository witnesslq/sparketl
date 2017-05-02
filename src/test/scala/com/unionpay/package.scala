package com.unionpay

import com.mongodb._
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.IndexOptions
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.sql._
import com.unionpay.util.ConfigUtil
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.bson.Document

import scala.collection.JavaConversions._

/**
  * Created by ywp on 2016/8/23.
  */
package object unionpay {

  import net.ceedubs.ficus.Ficus._
  import net.ceedubs.ficus.readers.ArbitraryTypeReader._

  private lazy val config = ConfigUtil.readClassPathConfig[MongoConfig]("mongo", "mongo")

  private lazy val uri = config.user match {
    case None => s"mongodb://${config.host}:${config.port}/${config.loginDb}"
    case Some(user) => s"mongodb://${config.user getOrElse ""}:${config.password getOrElse ""}@${config.host}:${config.port}/${config.loginDb}"
  }
  private lazy val mongoClientURI = new MongoClientURI(uri)
  private lazy val mongoClient = new MongoClient(mongoClientURI).getDatabase(config.db)


  def C(collection: String): MongoCollection[Document] = {
    val doc = mongoClient.getCollection(collection)
    doc
  }

  def createOrDefault2dIndex(collection: String, location: String = "location") = {
    val col = C(collection)
    try {
      if (col.listIndexes.filter(_.toJson.contains("location_2d")).isEmpty)
        col.createIndex(new BasicDBObject("location", "2d"))
    } catch {
      case e: Exception => println(s"you should check your 2d index"); throw e
    }
  }

  def createOrDefault2dSphereIndex(collection: String, location: String = "location") = {
    val col = C(collection)
    try {
      if (col.listIndexes.filter(_.toJson.contains("location_2dsphere")).isEmpty)
        col.createIndex(new BasicDBObject("location", "2dsphere"))
    } catch {
      case e: Exception => println(s"you should check your 2dsphere index"); throw e
    }
  }


  implicit class SqlContextReadMongo(@transient sqlContext: SQLContext) {


    def mongoDF(collection: String, shardKey: String = "_id"): DataFrame = {
      val conf = sqlContext.sparkContext.getConf
        .set("spark.mongodb.input.uri", uri)
        .set("spark.mongodb.input.database", config.db)
        .set("spark.mongodb.input.collection", collection)
        .set("spark.mongodb.input.partitioner", "MongoPaginateByCountPartitioner")
        .set("spark.mongodb.input.partitioner.partitionerOptions.numberOfPartitions", "6")
      val readConfig = ReadConfig(conf)
      sqlContext.read.mongo(readConfig)
    }

  }

  implicit class DataFrame2Mongo(df: DataFrame) {


    def save2Mongo(collection: String) {
      val conf = df.sqlContext.sparkContext.getConf
        .set("spark.mongodb.output.uri", uri)
        .set("spark.mongodb.output.database", config.db)
        .set("spark.mongodb.output.collection", collection)
      val writeConfig = WriteConfig(conf)
      df.write.mode(SaveMode.Overwrite).mongo(writeConfig)
    }

  }

}

case class MongoConfig(host: String, port: String = "27017", db: String, loginDb: String = "admin",
                       couponTable: String = "BankCouponInfo", shopTable: String = "MainShop5",
                       user: Option[String] = None, password: Option[String] = None)