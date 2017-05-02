package com.unionpay

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ywp on 2016/6/15.
  */
object ScalaDataFrame {


  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("scala")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val datas = Seq(ScalaChild(1, "a"), ScalaChild(1, "a"), ScalaChild(1, "a"))
    val df = datas.toDF("age", "name")
    val result = df.groupBy($"name").agg(sum($"age").as("totalAge"))
    result.show()
    result.printSchema()

    val ds = datas.toDS()
    val result2 = ds.groupBy(_.name).agg(sum($"age").as[Long])
    result2.show()
    result2.printSchema()



    sc.stop

  }


}

case class ScalaChild(age: Int, name: String)