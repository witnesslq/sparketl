package com.unionpay.util

import java.io.{BufferedReader, File, InputStreamReader}
import java.nio.file.{Files, Paths}

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.HanLP.Config
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
  * Created by ywp on 2016/8/6.
  */
object NLP {

  //商户名称无用字
  private lazy val usenessWs = Seq("美食", "汽车", "机构", "公司", "工作室", "营业部", "服务区", "批发部", "超市", "总代理", "广场",
    "信用社", "翡翠", "修理厂", "酒店", "股份有限公司", "经销部", "商店", "储蓄所", "经营部", "旅游", "有限责任公司", "西餐厅", "广场店",
    "公厕", "会馆", "有限公司", "直销处", "驿站", "专卖", "分公司", "合作社", "总店", "体验馆", "门市部", "营业所", "体育", "展示",
    "便利店", "事务所", "宴会厅", "中心", "科技", "洗手间", "办事处", "维修部", "农家乐", "生活馆", "分店", "啤酒", "餐厅",
    "邮政储蓄", "营业厅", "会所", "旗舰店", "支行", "连锁", "服务中心", "分社", "总经销", "婚礼策划", "专卖店", "基地", "婚纱摄影",
    "服饰", "服装", "贸易", "批发市场", "农产品", "商行", "云闪付", "﻿︰", "︳", "︴", "︵", "︶", "︷", "︸", "︹", "︺", "︻", "︼", "︽",
    "︾", "︿", "﹀", "﹁", "﹂", "﹃", "﹄", "﹉", "﹊", "﹋", "﹌", "﹍", "﹎", "﹏", "﹐", "﹑", "﹔", "﹕", "\\﹖", "﹝", "﹞", "﹟", "﹠",
    "﹡", "﹢", "﹤", "﹦", "﹨", "﹩", "﹪", "﹫", "！", "！", "！", "＂", "＇", "（", "）", "，", "：", "：", "；", "；", "？", "？", "？", "＿",
    "￣", "&nbsp;", "&nbsp", "\\(", "\\)", "【", "】", "●", "\\*", "＊", "、", "`", "\\'", "“", "”", "\"", "·", "。", "-","\\.","_"
  )

  private lazy val segment = HanLP
    .newSegment()
    .enableJapaneseNameRecognize(true)
    .enableNameRecognize(true)
    .enableTranslatedNameRecognize(true)

  //去掉末尾的数字
  private lazy val lastNumRex =
    """(.+?)(\d+)$"""
  //云闪付
  private lazy val yunRex =
    """[（ 云闪付 ） ]"""

  //无用字正则
  private lazy val wsRex =
    s"""${usenessWs.mkString("(", "|", ")")}"""

  implicit class WordSplit(word: String) {


    def isNumeric(str: String) = Try(str.toDouble).isSuccess

    /**
      * 得到分词后的词组
      *
      * @param flag 是否去掉商户名的无用字 lat 经纬度为0
      * @return
      */
    def wordSplit(flag: Boolean = false, lat: Double): Seq[String] = {
      Option(word) match {
        case None => Seq.empty[String]
        case Some(s) => {
          //如果最后一个是数字去掉
          val tmp = s.replaceAll(lastNumRex, "$1")
          //如果是用 () （）包裹并且里面是字母或者数字的去掉 肯德基广州番禺富丽KFC（GZH251）
          //            .replaceAll("（", "(").replaceAll("）", ")").replaceAll("(.+?)\\([a-zA-Z0-9]*\\)$", "$1")
          //针对云闪付的处理
          val wp = tmp.replaceAll(yunRex, "")
          val el = segment.seg(wp)
          val result = if (el.isEmpty) Seq.empty[String]
          else {
            //去除地名 经纬度为0的 不能去掉
            lat < 1 match {
              case true => {
                el.map(x => {
                  x.word.trim.replaceAll(" ", "")
                })
              }
              case false => {
                el
                  //保留外来词 去掉地名
                  //.filterNot(term => term.nature.name() == "nx")
                  .filterNot(_.nature.name() == "ns")
                  .map(x => {
                    x.word.trim.replaceAll(" ", "")
                  })
              }
            }

          }
          val res = flag match {
            case false => result
            case true => result.map(_.replaceAll(wsRex, "")).filterNot(_.isEmpty)
          }
          //如果分完词只剩一个或者空、用商户名以及品牌再填充一次
          if (res.size <= 1) {
            val buf = res.toBuffer
            buf.append(word.replaceAll(wsRex, "").replaceAll(" ", ""))
            buf.distinct
          } else res.distinct
        }
      }

    }


    def wordSplitCrawl: Seq[String] = {
      Option(word) match {
        case None => Seq.empty[String]
        case Some(s) => {
          //如果最后一个是数字去掉
          val tmp = s.replaceAll(lastNumRex, "$1")
          val wp = tmp.replaceAll(yunRex, "")
          val el = segment.seg(wp)
          val result = if (el.isEmpty) Seq.empty[String]
          else {
            el
              //保留外来词 去掉地名
              .filterNot(_.nature.name() == "ns")
              .map(x => {
                x.word.trim.replaceAll(" ", "")
              })
          }
          result.map(_.replaceAll(wsRex, "")).filterNot(_.isEmpty)
        }
      }

    }

    /**
      * 得到对应的词以及词性
      *
      * @return
      */
    def wordsNameNature: Seq[(String, String)] = {
      Option(word) match {
        case None => Seq.empty[(String, String)]
        case Some(s) => {
          val wd = s.trim
          val nList = segment.seg(wd)
          CoreStopWordDictionary.apply(nList)
          nList.map(term => {
            (term.word, term.nature.name())
          }).distinct
        }
      }
    }

    def replaceWord: String = {
      word.replaceAll(wsRex, "").replaceAll(" ", "").distinct
    }

  }

}
