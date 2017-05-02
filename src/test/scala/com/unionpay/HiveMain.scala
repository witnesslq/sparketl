package com.unionpay

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ywp on 2016/6/15.
  */
object HiveMain {


  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local[4]")
      .setAppName("hive")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val hiveContext = new HiveContext(sc)
    hiveContext.setConf("spark.sql.orc.filterPushdown", "true")
    import hiveContext.implicits._
    val orcUser = hiveContext.read.orc("D:\\test")
    orcUser.as('a).join(orcUser.as('b),$"a.Name"===$"b.Name","inner")
    orcUser.join(orcUser,orcUser("Name")===orcUser("Name"),"inner")
    //等价于下面写法
    orcUser.registerTempTable("user2")
    sc.stop()
  }


}
