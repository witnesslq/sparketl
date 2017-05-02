package com.scalatest

import com.unionpay.util.JsonUtil
import org.scalatest.FunSuite

/**
  * Created by ywp on 2016/7/27.
  */
class CardBounded extends FunSuite {

  def cleanTrade(lines: Seq[String]): Seq[Line] = {
    val line = lines.map(_.split("_")).map {
      case Array(a, b) => (a, b)
    }.sortBy(_._2)
    val indexLine = line.zipWithIndex.map(_.swap)
    for ((index, content) <- indexLine) yield {
      content match {
        case (uid, date) if index < line.size - 1 => Line(uid, date, indexLine(index + 1)._2._2)
        case (uid, date) if index == line.size - 1 => Line(uid, date, date)
      }
    }
  }

  test("卡解绑测试") {
    val trade = Seq(
      ("user1", "card1", "2016-07-02"),
      ("user2", "card1", "2016-08-03"),
      ("user1", "card1", "2016-08-04"),
      ("user2", "card1", "2016-08-05"),
      ("user3", "card2", "2016-07-03"),
      ("user4", "card2", "2016-07-07"),
      ("user5", "card3", "2016-07-07")
    )

    val groupCard = trade.groupBy(_._2).mapValues {
      case seq => {
        val tmp = seq.map(t => s"${t._1}_${t._3}")
        cleanTrade(tmp)
      }
    }

    println(JsonUtil.toJson4sPrettyString(groupCard))

  }
}

case class Line(uid: String, start: String, end: String)