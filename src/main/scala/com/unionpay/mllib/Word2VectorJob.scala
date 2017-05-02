package com.unionpay.mllib

import java.lang.Double

import ch.hsr.geohash.GeoHash
import com.unionpay.util.NLP._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{Normalizer, Word2Vec}
import org.apache.spark.mllib.linalg.{Vector, DenseVector => DV}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.unionpay.util.MathUtil._

/**
  * Created by ywp on 2016/9/2.
  */
object Word2VectorJob {


  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("UnionSelfShopDistinct--CrawlShopWordsVectorJob")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.yarn.driver.memoryOverhead", "1024")
      .set("spark.yarn.executor.memoryOverhead", "2000")
      .registerKryoClasses(Array(classOf[GeoLocation]))

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)

    sqlContext.setConf("spark.sql.shuffle.partitions", "25")
    //商户名分词 去掉无用字
    sqlContext.udf.register("sp", (sp: String, lat: Double) => sp.wordSplit(flag = true, lat))
    //geoHash 算法
    sqlContext.udf.register("geoHash", (lat: Double, lng: Double) => GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 12))
    //品牌是否添加
    sqlContext.udf.register("unionBS", (shopName: String, brandName: String) => {
      if (checkIfAddBrand(brandName)) brandName + shopName else shopName
    })

    //vector转化为 Seq[Double]
    sqlContext.udf.register("vec2Array", (v1: Vector) => v1.toDense.values)

    sqlContext.udf.register("similarity", (d1: DV, d2: DV) => calculateSimilarity(d1.toSparse, d2.toSparse))

    val word2Vec = new Word2Vec()
      .setInputCol("spName")
      .setOutputCol("nameVec")
      //todo
      .setVectorSize(30)
      .setMinCount(0)
      //todo
      .setNumPartitions(4)

    val normalizer = new Normalizer()
      .setInputCol("nameVec")
      .setOutputCol("normName")

    val pipeLine = new Pipeline().setStages(Array(word2Vec, normalizer))

    CrawlShopWordsVectorJob.doIt(pipeLine)

    UnionShopDistinctJob.doIt(pipeLine)


    sc.stop()
  }


}
