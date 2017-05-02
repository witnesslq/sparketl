package com.unionpay.test.ls

import org.apache.spark.mllib.linalg.{DenseVector => DV}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by ywp on 2016/8/22.
  */
object TestSim {

  private lazy val shopTmpModelPath = "C:\\Users\\ls\\Desktop\\sim\\model\\shopModel"
  private lazy val unionshopMongoModel = "unionShopGeo"
  private lazy val similarityPath = "C:\\Users\\ls\\Desktop\\sim\\shop\\similarity"


  private lazy val rex1 = """[0-9]+[元减 减 立减 再减].?""".r
  private lazy val rex2 ="""[0-9]+折""".r
  private lazy val rex3 ="""^-?[1-9]\d*$""".r

  private lazy val crawlshopMongoModel = "crawlShopGeo"
  private lazy val shopCrawlModel = "C:\\Users\\ls\\Desktop\\sim\\shop\\unioncrawlsimilarity"


  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("UnionSelfShopDistinct")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.yarn.driver.memoryOverhead", "1024")
      .set("spark.yarn.executor.memoryOverhead", "2000")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")

    val df = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\test_08_25\\model\\shopModel_test")
      .filter($"shopNo".isin(Seq("T00000000236826", "T00000000238022"): _*))
    df.printSchema()


    df.show()


    sc.stop()
  }

}