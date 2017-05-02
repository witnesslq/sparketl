package com.unionpay.util

import com.redislabs.provider.redis._
import com.redislabs.provider.redis.rdd.RedisKeysRDD
import org.apache.spark.{SparkConf, SparkContext}
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.Ficus._
import org.apache.spark.rdd.RDD

/**
  * Created by ywp on 2016/7/28.
  */
object RedisOps {


  private lazy val config = ConfigUtil.readClassPathConfig[RedisConfig]("redis", "config")

  implicit class RedisConfigBuild(conf: SparkConf) {

    def buildRedis: SparkConf = {

      conf.set("redis.host", config.host)
        .set("redis.auth", config.auth.getOrElse(""))
        .set("redis.port", config.port)
        .set("redis.db", config.db)
    }

    def getInitHost: (String, Int) = {
      (config.host, config.port.toInt)
    }
  }

  implicit class RedisReadRdd(sc: SparkContext) {

    def keyRDD(key: String, partitionNum: Int = 3): RedisKeysRDD = {
      val redisContext = new RedisContext(sc)
      redisContext.fromRedisKeyPattern((config.host, config.port.toInt), key, partitionNum)
    }
  }


}

case class RedisConfig(host: String, port: String = "6379", db: String = "1", auth: Option[String] = None)