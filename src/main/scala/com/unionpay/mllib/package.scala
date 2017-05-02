package com.unionpay

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{Normalizer, Word2Vec}

/**
  * Created by ywp on 2016/9/2.
  */
package object mllib {


  private lazy val rex1 = """[0-9]+[元减 减 立减 再减].?""".r
  private lazy val rex2 ="""[0-9]+折""".r
  private lazy val rex3 ="""^-?[1-9]\d*$""".r

  def checkIfAddBrand(brandName: String): Boolean = {
    if (brandName.trim.isEmpty || brandName == null) return false
    val seq = Seq("专享", "银联", "券", "观影", "银行", "邮储", "IC卡", "信用卡", "优惠", "6+2", "权益", "活动", "测试", "扫码", "积分", "信用卡", "重阳", "62", "六二", "悦享", "测试", "一元风暴", "约惠星期六")
    var f = true
    if (seq.toStream.map(brandName.contains).find(x => x) getOrElse false) f = false
    else if (rex1.findFirstIn(brandName).isDefined || rex2.findFirstIn(brandName).isDefined || rex3.findFirstIn(brandName).isDefined) f = false
    f
  }


  /*private lazy val shopNamew2c = "/mllib/pipeline/shopnameword2cev"

  type InputShopName = String

  implicit class Word2VecShopName(shopName: InputShopName) {

    def getOrCreatePipe(outPutShopName: String)(implicit sc: SparkContext): Pipeline = {

      val hadoopConf = sc.hadoopConfiguration
      val fs = FileSystem.get(hadoopConf)
      //先检查 hdfs是否训练好了模型
      if (!fs.exists(new Path(shopNamew2c))) {
        val word2Vec = new Word2Vec()
          .setInputCol(shopName)
          .setOutputCol("nameVec")
          //todo
          .setVectorSize(30)
          .setMinCount(0)
          //todo
          .setNumPartitions(4)

        val normalizer = new Normalizer()
          .setInputCol("nameVec")
          .setOutputCol(outPutShopName)

        val pipeLine = new Pipeline().setStages(Array(word2Vec, normalizer))
        pipeLine.write.save(shopNamew2c)
        pipeLine
      } else {
        val pipeline = Pipeline.read.load(shopNamew2c)
        pipeline
      }

    }


  }
*/

}
