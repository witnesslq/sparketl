package com.unionpay.test.ls

import com.databricks.spark.csv._
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ls on 2016/10/24.
  */
object DataDistinct {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("DataDistinct === 数据去重")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "32")

    val path = "/tmp/ls/flickr.csv"

    val fromDF = sqlContext.csvFile(path, true, ' ')
      .withColumnRenamed("%", "fromId")
      .withColumnRenamed("asym", "desId")


    //    val fromIdDF = fromDF.select("fromId")
    //    val desIdDF = fromDF.select("desId")
    //    val df = fromIdDF.unionAll(desIdDF).distinct()

    val desDF = fromDF.selectExpr("desId", "fromId")
    val df = fromDF.unionAll(desDF)
      .distinct()

    df.write.mode(SaveMode.Overwrite).parquet("/tmp/ls/data")

    sc.stop()
  }

}
