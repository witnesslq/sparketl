package com.unionpay.test

import com.unionpay.util.NLP._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{Normalizer, Word2Vec}
import org.apache.spark.mllib.linalg.{Vector, Vectors, DenseVector => DV, SparseVector => SV}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.unionpay.util.MathUtil._

/**
  * Created by ywp on 2016/9/1.
  */
object Word2VecTest {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("CrawlShopDistinctJob")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "4")
    sqlContext.udf.register("vec2Array", (v1: Vector) => v1.toDense.values)
    sqlContext.udf.register("sp", (sp: String) => sp.wordSplit(true, 111))
    sqlContext.udf.register("similarity", (v1: Seq[Double], v2: Seq[Double]) => {
      calculateSimilarity(Vectors.dense(v1.toArray).toSparse, Vectors.dense(v2.toArray).toSparse)
    })

    val shopDF = Seq((1, "上海同泰北路分店"), (2, "宝山保洁(同泰北路店)"))
      .toDF("shopId", "shopName")
      .selectExpr("shopId", "shopName", "sp(shopName) spName")

    val pipeLine: Pipeline = pipe

    val modelDF = pipeLine.fit(shopDF).transform(shopDF)
      .selectExpr("shopId", "shopName", "vec2Array(normName) normName", "spName")
    modelDF.as('a)
      .join(modelDF.as('b), $"a.shopId" !== $"b.shopId")
      .selectExpr("similarity(a.normName,b.normName) sim", "a.shopId", "a.shopName", "a.normName normName", "a.spName",
        "b.shopId", "b.shopName", "b.normName", "b.spName"
      )
      .show()
  }

  def pipe: Pipeline = {
    val word2Vec = new Word2Vec()
      .setInputCol("spName")
      .setOutputCol("nameVec")
      //todo
      .setVectorSize(30)
      .setMinCount(0)
      //todo
      .setNumPartitions(1)

    val normalizer = new Normalizer()
      .setInputCol("nameVec")
      .setOutputCol("normName")

    val pipeLine = new Pipeline().setStages(Array(word2Vec, normalizer))
    pipeLine
  }
}
