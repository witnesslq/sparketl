package com.scalatest

import org.scalatest.FunSuite

import scala.annotation.tailrec

/**
  * Created by ywp on 2016/8/25.
  */
class RemainText extends FunSuite {

  test("留存") {
    implicit val order = new Ordering[String] {
      override def compare(x: String, y: String): Int = x.compareTo(y)
    }
    val users = Seq(Remain(1, Seq("2016-01-01", "2016-02-01", "2016-03-01", "2016-04-01")),
      Remain(2, Seq("2016-02-01", "2016-03-01", "2016-04-01", "2016-05-01")),
      Remain(3, Seq("2016-06-01", "2016-02-01", "2016-03-01", "2016-04-01", "2016-05-01"))
    )
    val firstTime = users.groupBy(_.shoppingList.min).mapValues(_.map(_.uerId))
    //    val sortedTime = users.map(x => (x.uerId, x.shoppingList.sorted))
    val timeRange = users.map(_.shoppingList).flatten.distinct.sorted
    for (
      date <- timeRange
    ) yield {
      val thisDate = users.filter(_.shoppingList.contains(date)).map(_.uerId).distinct
      firstTime.getOrElse(date, Seq.empty[Int])
    }


  }

}

case class Remain(uerId: Int, shoppingList: Seq[String])
