package com.scalatest

import java.io.PrintWriter

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary
import com.hankcs.hanlp.tokenizer.{NLPTokenizer, StandardTokenizer}
import com.unionpay.util.JsonUtil
import org.scalatest.FunSuite

import scala.collection.JavaConversions._
import scala.io.Source

/**
  * Created by ywp on 2016/8/6.
  */
class HanNLPTest extends FunSuite {

  def wordsSplit(text: String): Seq[(String, String)] = {
    val seg = HanLP.newSegment()
    //      .enableAllNamedEntityRecognize(true)
    val nList = seg.seg(text)
    CoreStopWordDictionary.apply(nList)
    nList.filterNot(_.nature.name() == "ns").map(term => {
      (term.word, term.nature.name())
    })
  }

  test("互联网分词") {
    val text = Seq("肯德基(番禺富丽KFC店)", "肯德基广州番禺富丽KFC（GZH251）")
    text
      .map(x => x.replaceAll("（", "(").replaceAll("）", ")").replaceAll("(.+?)\\([a-zA-Z0-9]*\\)$", "$1"))
      .map(wordsSplit)
      .foreach(println)

  }


  test("标点符号") {
    val inValidWs = Seq("%", "'")
    val words = Seq("，", ",", "!", "！", "%", ";", "'", " ")
    words.foreach(x => {
      val ws = wordsSplit(x)
      val v = if (ws.size == 1 && ws.map(_._2).contains("w")) 0
      else if (x.size == 1) 0
      else if (ws.containsSlice(inValidWs)) 0
      else 1
      println(x, ws, v)
    })


  }

  test("测试分词") {

    val text = Seq(
      "泰飞唐亚洲精选Toffy Asian Cuisine", "国投财富广场2号写字楼1711-1714个体马强",
      "北京丰台造甲街110号48号楼311室个体郭明艳",
      "国投财富广场2号写字楼1711-1714个体马龙", "北京朝阳工人体育场北8-1-1层1108个体王伟",
      "密云投资促进局办公楼305室1043个体张金勇", "Gossip Girl Café绯蜜咖啡轻食馆", "kaldi koffie&thee（高地咖啡）",
      "L＇ame express 芳芳法式简餐厅", "“Tous Les Jours”(多乐之日)", "长百大楼年中庆银联IC卡闪付满300再减60",
      "PERSONAL POINT（北城天街专享）", "北京东城东四六条友诚商务楼4024个体崔丁炜",
      "百荣世贸商城市场F3层1街041号个体刘阳洲", "银联62儿童日，商场超市满30元立减11元", "中青旅牡丹江包机往返机票6.2折优惠券",
      "肯德基", "老干妈", "迪士尼", "拜耳", "乔丹", "李宁", "奥迪", "耐克", "奥迪", "15cm花屋"
    )
    text.foreach(x => println(wordsSplit(x)))
    //    val segment = HanLP.newSegment().enableAllNamedEntityRecognize(true)
    //    val el = segment.seg(text)
    //    el.map(_.word).foreach(println)
  }

  test("清洗银联自有的品牌数据") {
    import com.unionpay.util.NLP._

    def checkIfBrand(w: String): Boolean = {
      if (w.trim.isEmpty || w == null) return false
      val rex1 = """[0-9]+[元减 减 立减 再减].?""".r
      val rex2 ="""[0-9]+折""".r
      val rex3 ="""^-?[1-9]\d*$""".r
      val seq = Seq("银联", "券", "观影", "银行", "邮储", "IC卡", "信用卡", "优惠", "6+2", "权益", "活动", "测试", "扫码", "积分", "信用卡", "重阳", "62", "六二", "悦享", "测试", "一元风暴", "约惠星期六")
      var f = true
      if (seq.toStream.map(w.contains).find(x => x) getOrElse false) f = false
      else if (rex1.findFirstIn(w).isDefined || rex2.findFirstIn(w).isDefined || rex3.findFirstIn(w).isDefined) f = false
      f
    }

    val addBrand = (shopName: String, brandName: String) => {
      if (checkIfBrand(brandName)) brandName + shopName else shopName
    }

    val words = (sp: String, lat: Double) => sp.wordSplit(true, lat)

    val data = Seq(
      (0.000000, "银泰百货江东店3", "银泰百货"),
      (0.000000, "银泰百货江东店3", "重阳节百货商场"),
      (0.000000, "红蜻蜓（重阳）", "上海三林专卖店"),
      (1.1, "三林店", "银联钱包62营销活动通用券")

    )
      .map(x => (x._1, addBrand(x._2, x._3)))
      .map(x => words(x._2, x._1))
      .foreach(println)


  }

  test("清除英文词") {
    import com.unionpay.util.NLP._
    val ws: String = ""
    val dd = ws.wordsNameNature
    println(dd.isEmpty)

  }

  test("正则") {
    val text = Seq("121921219", "长百大楼年中庆银联IC卡闪付满300再减60")
    val re ="""[0-9]+[元减 减 立减 再减][0-9]+""".r
    text.foreach(x => println(re.findFirstIn(x)))
  }

  test("xxxx") {
    def de(w: String): Boolean = {
      val rex1 = """[0-9]+[元减 减 立减 再减].?""".r
      val rex2 ="""[0-9]+折""".r
      val rex3 ="""^-?[1-9]\d*$""".r
      val seq = Seq("专享", "银联", "券", "观影", "银行", "邮储", "IC卡", "信用卡", "优惠", "权益", "活动", "测试", "扫码", "云闪付", "积分", "6+2", "信用卡", "重阳", "62", "六二", "悦享", "测试", "一元风暴", "约惠星期六")
      var f = true
      if (seq.toStream.map(w.contains).find(x => x) getOrElse false) f = false
      else if (rex1.findFirstIn(w).isDefined || rex2.findFirstIn(w).isDefined || rex3.findFirstIn(w).isDefined) f = false
      f
    }
    //    val ws = Seq("中行信用卡美食立减48", "株洲家润多超市一元风暴")
    //    ws.filter(de)
    //      .foreach(println)
    val x = "D:\\project\\sparketl\\out\\artifacts\\sparketl_jar\\银联品牌倒排.csv"
    val s = Source.fromFile(x, "UTF-8")
      .getLines()
      .toStream
      .filter(de)
      .foreach(println)

  }

  test("增加分词") {
    val ps = "C:\\Users\\ywp\\Desktop\\stopwords.txt"
    val ba = "C:\\Users\\ywp\\Desktop\\stopwords_didi.txt"
    val pw = new PrintWriter(ba, "UTF-8")
    Source.fromFile(ps)("UTF-8")
      .getLines()
      .toStream
      .flatMap(x => {
        val ns = wordsSplit(x)
          .filter(_._2 == "ns")
          .map(_._1)
          .toBuffer
        ns.append(x)
        ns
      })
      .toSet[String]
      .foreach(x => pw.println(x))
    pw.close()
  }

  test("!!!") {
    val re = """(.+?)(\d+)$"""
    val word = "11小星星111"
    val res = word.replaceAll(re, "$1")
    println(res)
  }

  test("xxxxxx") {
    val seq = Seq("银联", "券", "观影", "银行", "邮储", "IC卡", "信用卡", "优惠", "6+2", "权益", "活动", "测试", "扫码", "积分", "信用卡", "重阳", "62", "六二", "悦享", "测试", "一元风暴", "约惠星期六")
    val rex =s"""[${seq.mkString("")}]""".r
    val name = ""
    val mt = rex.findFirstIn(name)
    println(mt)
    println(mt.isDefined)
  }

  test("sort") {

    implicit val orderingDouble = new Ordering[Double] {
      override def compare(x: Double, y: Double): Int = y.compareTo(x)
    }

    Source.fromFile("D:\\project\\sparketl\\target\\scala-2.10\\sim_crawl_shop.csv")
      .getLines()
      .toStream
      .drop(1)
      .map(x => {
        val Seq(sim1, sim2, a, b, c, d, e, f, g) = x.replaceAll(" ", "").replaceAll(", ", "").split("\t").toSeq
        (sim1, sim2.toDouble, a, b, c, d, e, f, g)
      }).sortBy(_._2)
      .foreach(println)
  }

  test("sp word") {
    import com.unionpay.util.NLP._
    val a = "上海威斯汀大饭店"
    val b = "威斯汀大饭店"

    println("自有:" + a.wordSplit(true, 111))
    println("互联网:" + b.wordSplit(true, 11))


    def comm(a: String, b: String) = a.intersect(b).size / scala.math.max(a.size, b.size).toDouble

    println(comm(a, b))

  }

  test("字符串去重") {
    import com.unionpay.util.NLP._
    val x = "大观园店公司金商公路"
    val ws = Seq("商店", "公关", "店", "公司")
    val res = x.replaceWord
    println(res)
  }

}
