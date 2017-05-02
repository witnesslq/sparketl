package com.unionpay

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}

import com.sparktest.CsvBaseTest
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import org.scalatest.FunSuite

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by ywp on 2016/11/13.
  */
class UnionShop extends FunSuite with CsvBaseTest {

  def writeTocsv(root: String, fn: String, df: DataFrame, deli: String = "\t") = {
    val fileName = if (fn.trim.isEmpty) "xxx" else fn.trim
    val pw = new PrintWriter(s"$root\\${if (fileName.endsWith(".csv")) fileName else fileName + ".csv"}", "UTF-8")
    val fields = df.schema.fieldNames
    pw.println(fields.mkString(deli))
    df.map(_.mkString(deli)).collect().foreach(pw.println)
    pw.close()
  }

  def reNameDF(df: DataFrame): DataFrame = {
    val c = df.schema.fieldNames.map(x => {
      (x, x
        .replaceAll("\\(", "").replaceAll("\\)", "").replaceAll("\\（", "").replaceAll("\\）", "")
        .replaceAll(".0", "").replaceAll(".1", "")
        .trim.replaceAll(" ", ""))
    })

    var tmpDF = df
    for ((oldName, newName) <- c) {
      tmpDF = tmpDF.withColumnRenamed(oldName, newName)
    }
    tmpDF
  }

  val fuckName = udf { (shopName: String, brandName: String) =>
    val sn = shopName.trim.replaceAll(" ", "")
    val bn = brandName.trim.replaceAll(" ", "")
    if (sn.contains(bn)) sn else s"$bn($sn)"
  }
  val caoName = udf { (o: String, n: String) =>
    val on = o.trim.replaceAll(" ", "")
    val nn = n.trim.replaceAll(" ", "")
    if (nn.isEmpty) on else nn
  }

  test("--8.31") {
    sqlContext.udf.register("dl", (n: String) => {
      n.trim.replaceAll(" ", "") match {
        case "" | "2.0" | "2" => ""
        case x@_ => x
      }
    })
    import sqlContext.implicits._
    val dfs = mutable.Buffer[DataFrame]()
    Files.list(Paths.get("C:\\Users\\ywp\\Desktop\\吃瓜群众新作\\1.4 门店地址编号\\csv"))
      .iterator()
      .toStream
      .foreach(csv => {
        val file = csv.toString
        println(file)
        val df = csvContext.csvFile(
          filePath = file,
          charset = "UTF-8"
        )
          .filter(!$"商户名称(MCHNT_NM)".contains("验证"))
          .filter(!$"商户名称(MCHNT_NM)".contains("测试"))
          .withColumnRenamed("商户代码(MCHNT_CD)", "MCHNT_CD")
          .withColumnRenamed("商户名称(MCHNT_NM)", "MCHNT_NM")
          .withColumnRenamed("商户名称", "NEW_NAME")
          .withColumnRenamed("品牌ID(BRAND_ID)", "BRAND_ID")
          .withColumnRenamed("品牌名(BRAND_NM)", "BRAND_NM")
          .withColumnRenamed("品牌名", "NEW_BRAND")
          .withColumnRenamed("品牌名 ", "NEW_BRAND")
          .selectExpr("MCHNT_CD", "MCHNT_NM", "dl(NEW_NAME) NEW_NAME",
            "BRAND_ID", "BRAND_NM", "dl(NEW_BRAND)  NEW_BRAND"
          )
        dfs.append(df)
        println(file)
      })

    var sDF = dfs.head
    for (df <- dfs.tail) sDF = sDF.unionAll(df)
    sDF.printSchema()
    sDF.coalesce(6).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ywp\\Desktop\\数据\\shop\\0831")
  }


  test("9.01--10.25") {
    sqlContext.udf.register("dl", (n: String) => {
      n.trim.replaceAll(" ", "") match {
        case "" | "2.0" | "2" => ""
        case x@_ => x
      }
    })
    import sqlContext.implicits._
    val dfs = mutable.Buffer[DataFrame]()
    Files.list(Paths.get("C:\\Users\\ywp\\Desktop\\数据\\门店901-1025\\csv"))
      .iterator()
      .toStream
      .foreach(csv => {
        val file = csv.toString
        println(file)
        val df = csvContext.csvFile(
          filePath = file,
          charset = "UTF-8"
        )
          .filter(!$"商户名称(MCHNT_NM)".contains("验证"))
          .filter(!$"商户名称(MCHNT_NM)".contains("测试"))
          .withColumnRenamed("商户代码(MCHNT_CD)", "MCHNT_CD")
          .withColumnRenamed("商户名称(MCHNT_NM)", "MCHNT_NM")
          .withColumnRenamed("商户名称", "NEW_NAME")
          .withColumnRenamed("品牌ID(BRAND_ID)", "BRAND_ID")
          .withColumnRenamed("品牌名(BAND_NM)", "BRAND_NM")
          .withColumnRenamed("品牌名", "NEW_BRAND")
          .selectExpr("MCHNT_CD", "MCHNT_NM", "dl(NEW_NAME) NEW_NAME",
            "BRAND_ID", "BRAND_NM", "dl(NEW_BRAND)  NEW_BRAND"
          )
        dfs.append(df)
        println(file)
      })

    var sDF = dfs.head
    for (df <- dfs.tail) sDF = sDF.unionAll(df)
    sDF.printSchema()
    sDF.coalesce(6).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ywp\\Desktop\\数据\\shop\\9-10")
  }

  test("union 0831 9-10") {
    import sqlContext.implicits._
    val xUDF = udf((s: String) => if (s.endsWith(".0")) s.substring(0, s.size - 2) else s)
    val df1 = sqlContext.read.parquet("C:\\Users\\ywp\\Desktop\\数据\\shop\\0831")
    val df2 = sqlContext.read.parquet("C:\\Users\\ywp\\Desktop\\数据\\shop\\9-10")
    val df = df1.unionAll(df2).withColumn("BRAND_ID", xUDF($"BRAND_ID"))
    df.write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ywp\\Desktop\\数据\\shop\\parquet_shop")
  }

  test("update brandname") {
    val df = sqlContext.read.parquet("C:\\Users\\ywp\\Desktop\\数据\\shop\\parquet_shop")
      .filter("MCHNT_CD='T00000000268040'")
    //      .selectExpr("BRAND_NM", "NEW_BRAND")
    df.show(1000)
    /*val pw = new PrintWriter("C:\\Users\\ywp\\Desktop\\周一\\品牌\\updateBrandName.sql", "UTF-8")
    df.map(row => {
      val o = row.getAs[String]("BRAND_NM")
      val n = row.getAs[String]("NEW_BRAND")
      (o, n)
    })
      .collect()
      .foreach { case (t1, t2) => {
        pw.println(s"update tbl_content_brand_inf_tmp set brand_nm='$t2' where brand_nm='$t1' ;")
      }
      }
    pw.close()*/
  }
  test("一二级分类") {
    val file = "C:/Users/ywp/Desktop/商品二级分类.csv"
    val df = csvContext.csvFile(
      filePath = file,
      charset = "UTF-8"
    ).withColumnRenamed("票券ID", "src_goods_no")
      .withColumnRenamed("二级分类", "sub_industry_no")
    df.printSchema()

    df.map(r => {
      val id = r.getAs[String]("src_goods_no")
      val su = r.getAs[String]("sub_industry_no")
      s"update tbl_content_goods_inf set sub_industry_no='$su' where src_goods_no='$id';"
    })
      .foreach(println)
  }
  test("goods简洁") {
    val file = "C:/Users/ywp/Desktop/1025前所有.csv"
    val df = csvContext.csvFile(
      filePath = file,
      charset = "UTF-8"
    )
      .withColumnRenamed("票券ID", "src_goods_no")
      .withColumnRenamed("票券简介", "goods_intro")
    df.printSchema()

    val pw = new PrintWriter("C:\\Users\\ywp\\Desktop\\周一\\update_goods_intro.sql", "UTF-8")
    df.map(r => {
      val id = r.getAs[String]("src_goods_no")
      val su = r.getAs[String]("goods_intro")
      s"update tbl_content_goods_inf set goods_intro='$su' where src_goods_no='$id';"
    })
      .collect()
      .foreach(pw.println)

    pw.close()
  }

  test("活跃品牌") {
    val file = "C:\\Users\\ywp\\Desktop\\数据\\aa.csv"
    val df = csvContext.csvFile(
      filePath = file,
      charset = "UTF-8"
    )
      .withColumnRenamed("新品牌ID", "brand_no")
      .selectExpr("brand_no")
      .distinct()

    println(df.count())
    df.repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ywp\\Desktop\\数据\\brand")
  }

  test("门店parquet") {
    import sqlContext.implicits._
    val file = "C:\\Users\\ywp\\Desktop\\数据\\export_shop.csv"
    val df = csvContext.csvFile(
      filePath = file,
      charset = "UTF-8"
    )
      .withColumnRenamed("品牌Id", "brand_no")
      .withColumnRenamed("门店Id", "src_shop_no")
    val brandDF = sqlContext.read.parquet("C:\\Users\\ywp\\Desktop\\数据\\brand")
    val x = df.as('a)
      .join(brandDF.as('b), $"a.brand_no" === $"b.brand_no", "leftsemi")
      .selectExpr("a.*")
    x.write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ywp\\Desktop\\数据\\activity_shop")
  }

  test("活跃门店") {
    import sqlContext.implicits._
    import org.apache.spark.sql.functions._

    val root = "C:\\Users\\ywp\\Desktop\\数据\\活跃门店"
    def write2csv(fileName: String, df: DataFrame) = {
      val pw = new PrintWriter(s"$root\\$fileName", "UTF-8")
      val fields = df.schema.fieldNames
      pw.println(fields.mkString(","))
      df.map(_.mkString(",")).collect().foreach(pw.println)
      pw.close()
    }

    val paths = Files
      .list(Paths.get("C:\\Users\\ywp\\Desktop\\数据\\临时处理\\csv"))
      .iterator()
      .toList
    val activityDF = sqlContext.read.parquet("C:\\Users\\ywp\\Desktop\\数据\\activity_shop_partition")

    paths.foreach(x => {
      val file = x.toString
      val fileName = x.getFileName.toString
      val df = csvContext.csvFile(
        filePath = file,
        charset = "UTF-8"
      )
        .withColumnRenamed("商户代码(MCHNT_CD)", "MCHNT_CD")

      val xDF = df.as('a)
        .join(activityDF.as('b), $"a.MCHNT_CD" === $"b.src_shop_no", "leftsemi")
        .selectExpr("a.*")
      write2csv(fileName, xDF)
    })

  }

  test("xxxx") {

    val paths = Files
      .list(Paths.get("C:\\Users\\ywp\\Desktop\\数据\\临时处理\\csv"))
      .iterator()
      .toList

    var count: Long = 0L
    paths.foreach(x => {
      val file = x.toString
      val fileName = x.getFileName.toString
      val df = csvContext.csvFile(
        filePath = file,
        charset = "UTF-8"
      ).withColumnRenamed("商户代码(MCHNT_CD)", "MCHNT_CD")
        .selectExpr("MCHNT_CD").distinct()
      count += df.count()
    })
    println("总数：" + count)

  }

  test("总公司") {
    val file = "C:\\Users\\ywp\\Desktop\\数据\\活跃品牌\\csv\\总公司.csv"
    val df = csvContext.csvFile(
      filePath = file,
      charset = "UTF-8"
    )
      .withColumnRenamed("新品牌id", "brand_no")
      .selectExpr("brand_no")
    df.repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ywp\\Desktop\\数据\\活跃品牌\\activity_brand")
  }

  test("补录活跃门店") {

    import sqlContext.implicits._
    import org.apache.spark.sql.functions._

    val root = "C:\\Users\\ywp\\Desktop\\数据\\活跃门店_2"
    def write2csv(fileName: String, df: DataFrame) = {
      val pw = new PrintWriter(s"$root\\$fileName", "UTF-8")
      val fields = df.schema.fieldNames
      pw.println(fields.mkString(","))
      df.map(_.mkString(",")).collect().foreach(pw.println)
      pw.close()
    }

    val paths = Files
      .list(Paths.get("C:\\Users\\ywp\\Desktop\\数据\\临时处理\\csv"))
      .iterator()
      .toList
    val activityDF = sqlContext.read.parquet("C:\\Users\\ywp\\Desktop\\数据\\activity_shop_brand")

    paths.foreach(x => {
      val file = x.toString
      val fileName = x.getFileName.toString
      val df = csvContext.csvFile(
        filePath = file,
        charset = "UTF-8"
      ).withColumnRenamed("商户代码(MCHNT_CD)", "MCHNT_CD")

      val xDF = df.as('a)
        .join(activityDF.as('b), $"a.MCHNT_CD" === $"b.src_shop_no", "leftsemi")
        .selectExpr("a.*")
      write2csv(fileName, xDF)
    })


  }

  test("10.25-11.11门店") {
    import sqlContext.implicits._

    val root = "C:\\Users\\ywp\\Desktop\\10.25-11.11门店\\csv"
    def write2csv(fn: String, df: DataFrame) = {
      val fileName = if (fn.trim.isEmpty) "xxx" else fn.trim
      val pw = new PrintWriter(s"$root\\${if (fileName.endsWith(".csv")) fileName else fileName + ".csv"}", "UTF-8")
      val fields = df.schema.fieldNames
      pw.println(fields.mkString("\t"))
      df.map(_.mkString("\t")).collect().foreach(pw.println)
      pw.close()
    }

    val file = "C:\\Users\\ywp\\Desktop\\10.25-11.11门店\\shop_1025-11.10.csv"

    val fuck = udf { (code: String) =>
      if (code.endsWith(".0")) code.substring(0, code.size - 2) else code
    }

    val df = csvContext.csvFile(
      filePath = file,
      charset = "UTF-8"
    )
      .withColumnRenamed("所属银联分公司代码(CUP_BRANCH_INS_ID_CD)", "CUP_BRANCH_INS_ID_CD")
      .withColumnRenamed("商户名称(MCHNT_NM)", "MCHNT_NM")
      .withColumnRenamed("品牌名(BAND_NM)", "BAND_NM")
      .withColumn("CUP_BRANCH_INS_ID_CD", fuck(trim($"CUP_BRANCH_INS_ID_CD")))
      .withColumn("MCHNT_NM", fuckName($"MCHNT_NM", $"BAND_NM"))
      .withColumnRenamed("MCHNT_NM", "商户名称(MCHNT_NM)")
      .withColumnRenamed("BAND_NM", "品牌名(BAND_NM)")

    val codes = df.selectExpr("CUP_BRANCH_INS_ID_CD").distinct()
      .map(_.getAs[String]("CUP_BRANCH_INS_ID_CD"))
      .collect()

    codes.foreach(x => {
      write2csv(x, df.filter(s"CUP_BRANCH_INS_ID_CD='$x'")
        .withColumnRenamed("CUP_BRANCH_INS_ID_CD", "所属银联分公司代码(CUP_BRANCH_INS_ID_CD)"))
    })

  }
  test("fuck") {
    val root = "C:\\Users\\ywp\\Desktop\\10.25-11.11门店\\csv"
    val ds = ArrayBuffer[DataFrame]()
    Files.list(Paths.get(root))
      .iterator()
      .map(_.toString)
      .foreach(file => {
        val df = csvContext.csvFile(
          filePath = file,
          charset = "UTF-8",
          delimiter = '\t'
        )
        ds.append(df)
      })
    var sDF = ds.head
    for (x <- ds.tail) sDF = sDF.unionAll(x)
    println(sDF.count())
  }

  test("活跃品牌去重后处理") {
    import sqlContext.implicits._
    /*

     val ds = ArrayBuffer[(String, DataFrame)]()
     val fs = Files.list(Paths.get("C:\\Users\\ywp\\Desktop\\1.5相似完成\\csv"))
       .iterator()
       .toList
       .sortBy(_.getFileName.toString)
     fs.foreach(x => {
       val file = x.toString
       val name = x.getFileName.toString
       val xdf = csvContext.csvFile(
         filePath = file,
         charset = "UTF-8"
       )
       val df = reNameDF(xdf)
       ds.append((file, df))
     })
     var sDF = ds.head._2
     println(ds.head._1)
     for ((f, x) <- ds.tail) sDF = {
       println(f)
       sDF.unionAll(x)
     }
     sDF.repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ywp\\Desktop\\1.5相似完成\\parquet")*/
    val sb = udf((s: String) => {
      val sim = s.trim.replaceAll(" ", "")
      if (sim.endsWith(".0")) sim.substring(0, sim.size - 2) else sim
    })
    val df = sqlContext.read.parquet("C:\\Users\\ywp\\Desktop\\1.5相似完成\\parquet")
      .select('商户代码MCHNT_CD.as('src_shop_no), sb('相似).as('sim), '相似商户代码.as('sim_no))
    df.selectExpr("src_shop_no", "sim", "sim_no")
  }


  test("全量数据") {
    import sqlContext.implicits._

    /*def reNameDF(df: DataFrame): DataFrame = {
      val c = df.schema.fieldNames.map(x => {
        (x, x
          .replaceAll("\\(", "").replaceAll("\\)", "").replaceAll("\\（", "").replaceAll("\\）", "")
          .replaceAll(".0", "").replaceAll(".1", "")
          .trim.replaceAll(" ", ""))
      })

      var tmpDF = df
      for ((oldName, newName) <- c) {
        tmpDF = tmpDF.withColumnRenamed(oldName, newName)
      }
      tmpDF
    }

    val ds = ArrayBuffer[(String, DataFrame)]()
    val fs = Files.list(Paths.get("C:\\Users\\ywp\\Desktop\\1.5.1全量数据\\csv"))
      .iterator()
      .toList
      .sortBy(_.getFileName.toString)
    fs.foreach(x => {
      val file = x.toString
      val xdf = csvContext.csvFile(
        filePath = file,
        charset = "UTF-8"
      )
      val df = reNameDF(xdf)
      if (df.schema.fieldNames.size != 65) println("*" * 10 + file)
      ds.append((file, df))
    })
    var sDF = ds.head._2
    println(ds.head._1)
    for ((f, x) <- ds.tail) sDF = {
      println(f)
      sDF.unionAll(x)
    }
    sDF.repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ywp\\Desktop\\1.5.1全量数据\\parquet")*/
    val df = sqlContext.read.parquet("C:\\Users\\ywp\\Desktop\\1.5.1全量数据\\parquet")
    df.filter($"商户代码MCHNT_CD".isin(Seq("T0000000022864", "T00000000291501", "T00000000254818"): _*))
      .show()
  }


  test("相似活跃二次处理结果") {
    import sqlContext.implicits._


    val sb = udf { (x: String) =>
      val s = x.trim.replaceAll(" ", "")
      if (s.isEmpty) "1"
      else {
        s match {
          case "0" | "0.0" => "0"
          case _ => "1"
        }
      }
    }
    val root = "C:\\Users\\ywp\\Desktop\\1.5相似完成\\rename_shop"
    def write2csv(fn: String, df: DataFrame) = {
      val fileName = if (fn.trim.isEmpty) "xxx" else fn.trim
      val pw = new PrintWriter(s"$root\\${if (fileName.endsWith(".csv")) fileName else fileName + ".csv"}", "UTF-8")
      val fields = df.schema.fieldNames
      pw.println(fields.mkString("\t"))
      df.map(_.mkString("\t")).collect().foreach(pw.println)
      pw.close()
    }

    val allDF = sqlContext.read.parquet("C:\\Users\\ywp\\Desktop\\1.5.1全量数据\\parquet")
      .withColumn("商户代码MCHNT_CD", trim('商户代码MCHNT_CD))
      .withColumnRenamed("品牌名BAND_NM", "品牌名BRAND_NM")

    val xsbDF = sqlContext.read.parquet("C:\\Users\\ywp\\Desktop\\1.5相似完成\\parquet")
      .select(trim(sb('相似)).as('相似), trim('相似商户代码).as('相似商户代码), trim('商户代码MCHNT_CD).as('商户代码MCHNT_CD))

    val sbDF = xsbDF.filter('相似 === lit("0"))
      .selectExpr("相似商户代码", "商户代码MCHNT_CD")

    val noDF = sbDF.select('相似商户代码.as('sid)).distinct().except(xsbDF.select('商户代码MCHNT_CD.as('sid)).distinct())
    //    val interDF = sbDF.select('相似商户代码.as('sid)).distinct().intersect(xsbDF.select('商户代码MCHNT_CD.as('sid)).distinct())
    //    val appendDF = allDF.as('a)
    //      .join(noDF.as('b), $"a.商户代码MCHNT_CD" === $"b.sid", "leftsemi")
    //      .selectExpr("a.*")
    //      .withColumn("商户名称MCHNT_NM", fuckName(caoName('商户名称MCHNT_NM, '商户名称), caoName('品牌名BRAND_NM, '品牌名)))

    val df = xsbDF.select(trim('商户代码MCHNT_CD).as('src_shop_no), trim(sb('相似)).as('st))
    //    val exDF = noDF.select('sid.as('src_shop_no)).withColumn("st", lit("1"))
    //    val inDF = df.as('a)
    //      .join(interDF.as('b), $"a.src_shop_no" === $"b.sid", "leftsemi")
    //      .selectExpr("a.src_shop_no", "a.st", "'1' sst")
    //      .filter("st!=sst")

    //    write2csv("not_exists_activity", appendDF)

    /*def reNameDF(df: DataFrame): DataFrame = {
      val c = df.schema.fieldNames.map(x => {
        (x, x
          .replaceAll("\\(", "").replaceAll("\\)", "").replaceAll("\\（", "").replaceAll("\\）", "")
          .replaceAll(".0", "").replaceAll(".1", "")
          .trim.replaceAll(" ", ""))
      })

      var tmpDF = df
      for ((oldName, newName) <- c) {
        tmpDF = tmpDF.withColumnRenamed(oldName, newName)
      }
      tmpDF
    }
    val fs = Files.list(Paths.get("C:\\Users\\ywp\\Desktop\\1.5相似完成\\csv"))
      .iterator()
      .toList
      .sortBy(_.getFileName.toString)

    fs.foreach(x => {
      val file = x.toString
      val xdf = csvContext.csvFile(
        filePath = file,
        charset = "UTF-8"
      )
      val df = reNameDF(xdf)
        .withColumnRenamed("品牌名BAND_NM", "品牌名BRAND_NM")
        .withColumn("商户名称MCHNT_NM", fuckName(caoName('商户名称MCHNT_NM, '商户名称), caoName('品牌名BRAND_NM, '品牌名)))

      //      write2csv(x.getFileName.toString, df)
    })*/


  }

  test("标记为0的门店相似分公司分类") {
    val root = "C:\\Users\\ywp\\Desktop\\exclude"
    val xx = udf { (code: String) =>
      val c = code.trim.replaceAll(" ", "")
      if (c.endsWith(".0")) c.substring(0, c.size - 2) else c
    }
    import sqlContext.implicits._
    val df = csvContext.csvFile(
      filePath = "C:\\Users\\ywp\\Desktop\\not_exists_activity.csv",
      charset = "UTF-8",
      delimiter = '\t'
    )
      .withColumn("所属银联分公司代码CUP_BRANCH_INS_ID_CD", trim(xx('所属银联分公司代码CUP_BRANCH_INS_ID_CD)))
      .withColumn("商户名称MCHNT_NM", fuckName(caoName('商户名称MCHNT_NM, '商户名称), caoName('品牌名BRAND_NM, '品牌名)))

    val codes = df.select(trim('所属银联分公司代码CUP_BRANCH_INS_ID_CD).as('code)).distinct().map(_.getAs[String]("code")).collect()

    codes.foreach(x => {
      val xDF = df.filter(s"trim(所属银联分公司代码CUP_BRANCH_INS_ID_CD)='$x'")
      writeTocsv(root, if (x.isEmpty) "xxx" else s"aa$x", xDF)
    })

  }

  test("相似处理") {
    import sqlContext.implicits._


    /*val ds = ArrayBuffer[DataFrame]()
    Files.list(Paths.get("C:\\Users\\ywp\\Desktop\\1.5相似完成\\rename_shop\\csv"))
      .iterator()
      .toList
      .map(_.toString)
      .foreach(x => {
        val df = csvContext.csvFile(
          filePath = x,
          charset = "UTF-8",
          delimiter = '\t'
        ).withColumnRenamed("商户代码MCHNT_CD", "src_shop_no")
        ds.append(df)
      })

    var sDF = ds.head
    for (x <- ds.tail) sDF = sDF.unionAll(x)*/

    val xx = udf((s: String) => {
      val x = s.trim.replaceAll(" ", "")
      x match {
        case "0" | "0.0" => "0"
        case _ => "1"

      }
    })

    val df = sqlContext.read.parquet("C:\\Users\\ywp\\Desktop\\1.5相似完成\\parquet")
      .select(trim('商户代码MCHNT_CD).as('src_shop_no), xx(trim('相似)).as('shop_valid_st), trim('相似商户代码).as('sim_no),
        trim('商户名称MCHNT_NM).as('old_shop_name), trim('商户名称).as('new_shop_name), trim('品牌名BAND_NM).as('old_brand_name),
        trim('品牌名).as('new_brand_name)
      )

    val vDF = df.filter("shop_valid_st='1'").selectExpr("src_shop_no", "shop_valid_st")
    val nDF = df.filter("shop_valid_st='0'")
    val nDF1 = nDF.selectExpr("src_shop_no", "shop_valid_st")
    val vDF1 = nDF.selectExpr("sim_no src_shop_no").distinct()
    val vDF2 = vDF1.except(df.selectExpr("src_shop_no").distinct()).withColumn("shop_valid_st", lit("1"))
    val finalDF = vDF.unionAll(nDF1).unionAll(vDF2)
    finalDF.repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ywp\\Desktop\\activity_shop_valid_st")

    /*val XDF = df.selectExpr("src_shop_no", "old_shop_name", "new_shop_name", "old_brand_name", "new_brand_name")
   XDF.repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ywp\\Desktop\\activity_shop_new_sb_name")*/

    /*finalDF.selectExpr("src_shop_no").distinct().except(sDF.selectExpr("src_shop_no").distinct())
      .map(_.getAs[String]("src_shop_no"))
      .collect()
      .foreach(println)*/

    //    val xdf = sqlContext.read.parquet("C:\\Users\\ywp\\Desktop\\activity_shop_valid_st")
    //    println(xdf.selectExpr("src_shop_no").distinct.count)

  }

  test("门店、品牌处理") {
    import sqlContext.implicits._
    //    val cs = udf((x: String) => x.contains("-"))
    val shopDF = sqlContext.read.parquet("C:\\Users\\ywp\\Desktop\\production\\tbl_content_shop_inf")
      .select('brand_no, 'src_shop_no, 'shop_nm)
    val brandDF = sqlContext.read.parquet("C:\\Users\\ywp\\Desktop\\production\\tbl_content_brand_inf")
      .select('brand_no, 'src_brand_no, 'brand_nm)
    val shopBrandDF = shopDF.as('a)
      .join(brandDF.as('b), $"a.brand_no" === $"b.brand_no")
      .selectExpr("a.src_shop_no", "a.shop_nm", "coalesce(b.brand_no,'') brand_no",
        "coalesce(b.src_brand_no,'') src_brand_no", "coalesce(b.brand_nm,'') brand_nm")
    //      .filter(cs('src_brand_no))
    shopBrandDF.repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ywp\\Desktop\\production\\shop_brand")

  }

  test("外包修改品牌门店名与生产数据合并") {
    import sqlContext.implicits._
    val actShopDF = sqlContext.read.parquet("C:\\Users\\ywp\\Desktop\\activity_shop_new_sb_name")
    println(actShopDF.count())
    val proShopDF = sqlContext.read.parquet("C:\\Users\\ywp\\Desktop\\production\\shop_brand")
      .withColumnRenamed("shop_nm", "pro_shop_nm")
      .withColumnRenamed("brand_no", "pro_brand_no")
      .withColumnRenamed("src_brand_no", "pro_src_brand_no")
      .withColumnRenamed("brand_nm", "pro_brand_nm")
    println(proShopDF.count())
    //    val compactDF = proShopDF.as('b)
    //      .join(actShopDF.as('a), $"a.src_shop_no" === $"b.src_shop_no")
    //      .selectExpr("a.*", "b.pro_shop_nm", "b.pro_brand_no", "b.pro_src_brand_no", "b.pro_brand_nm")
    //    compactDF.repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ywp\\Desktop\\shop_brand_pro_sb_compact")
  }

  test("合并数据导出") {
    val root = "C:\\Users\\ywp\\Desktop\\外包处理与生产数据合并对比"
    val df = sqlContext.read.parquet("C:\\Users\\ywp\\Desktop\\shop_brand_pro_sb_compact")
    writeTocsv(root, "compact", df)
  }

  test("活跃门店状态sql") {
    import sqlContext.implicits._
    val xx = udf((x: Seq[String]) => x.mkString(","))
    val xdf = sqlContext.enableHive.read.parquet("C:\\Users\\ywp\\Desktop\\activity_shop_valid_st")
    //    val df = xdf.groupBy("src_shop_no").agg(xx(collect_set("shop_valid_st")).as("shop_valid_st"))
    val df = xdf.groupBy("src_shop_no").agg(collect_list("shop_valid_st").as("shop_valid_st"))
    val x = df.filter("size(shop_valid_st)>1")
    x.show(x.count().toInt)

    //    val pw = new PrintWriter("C:\\Users\\ywp\\Desktop\\update_shop_state.sql")
    //    pw.println(df.schema.fieldNames.mkString(","))
    //    df.map(_.mkString(",")).collect().foreach(pw.println)
    //    pw.close()
  }

  test("相似门店分组") {
    import sqlContext.implicits._
    val sb = udf { (x: String) =>
      val s = x.trim.replaceAll(" ", "")
      if (s.isEmpty) "1"
      else {
        s match {
          case "0" | "0.0" => "0"
          case _ => "1"
        }
      }
    }

    val allDF = sqlContext.enableHive.read.parquet("C:\\Users\\ywp\\Desktop\\1.5相似完成\\parquet")
      .select(trim(sb('相似)).as('sim), trim('相似商户代码).as('sim_no), trim('商户代码MCHNT_CD).as('src_shop_no))
    val simDF = allDF.filter("sim='0'")
      .selectExpr("src_shop_no  shopNo", "sim_no otherNo")
    val v = simDF.selectExpr("shopNo id").unionAll(simDF.selectExpr("otherNo id")).distinct().orderBy("id")
    val e = simDF.selectExpr("shopNo src", "otherNo dst").withColumn("relationship", lit("sim"))
    val g = GraphFrame(v, e)
    val result = g.connectedComponents.run()
    val sList = result.groupBy("component")
      .agg(collect_set("id") as ("ids"))
    sList.repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ywp\\Desktop\\1.5相似完成\\graph")
    /*val xsql = sqlContext.enableHive()
    val flatten = udf((xs: Seq[String]) => xs.mkString("|"))
    val fuck = udf((xs: Seq[String]) => xs.mkString("(", ",", ")"))
    val xDF = xsql.read.parquet("C:\\Users\\ywp\\Desktop\\1.5相似完成\\graph")
      //      .select('component.as('分组ID), flatten('dirtyIds).as('相似列表))
      .select('component.as('分组ID), 'dirtyIds.as('相似列表))
    //    writeTocsv("C:\\Users\\ywp\\Desktop\\1.5相似完成", "相似列表", xDF)
    val sbMb = xsql.read.parquet("C:\\Users\\ywp\\Desktop\\activity_shop_valid_st")
      .groupBy('src_shop_no).agg(fuck(collect_list('shop_valid_st)).as('sl))
      .map(r => (r.getAs[String]("src_shop_no"), r.getAs[String]("sl")))
      .collect()
      .toMap
    val bb = sc.broadcast(sbMb)
    val re = xDF.mapPartitions(it => {
      val bv = bb.value
      it.map(r => {
        val id = r.getAs[Long]("分组ID")
        val sl = r.getAs[Seq[String]]("相似列表")
        (id, sl.map(x => s"$x:${bv.getOrElse(x, "(null)")}").mkString("|"))
      })
    }).toDF("id", "ls")
    writeTocsv("C:\\Users\\ywp\\Desktop", "sim_graph", re)*/
  }

  /*  test("excel read") {
      val ds = ArrayBuffer[DataFrame]()
      /* Files.list(Paths.get("C:\\Users\\ywp\\Desktop\\1.5相似完成"))
         .iterator()
         .map(_.toString)
         .filter(_.endsWith(".xlsx"))
         .foreach(x => {
           println(x)
           val df = sqlContext.excelFile(x, "Sheet1")
           ds.append(df)
         })*/

      Seq("C:\\Users\\ywp\\Desktop\\1.5相似完成\\0901_1025号店铺补录.xlsx", "C:\\Users\\ywp\\Desktop\\1.5相似完成\\aa00010000.xlsx")
        .foreach(x => {
          println(x)
          val df = sqlContext.excelFile(x, "Sheet1")
          ds.append(df)
        })

      var sDF = ds.head
      for (x <- ds.tail) sDF = sDF.unionAll(x)
      sDF.repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ywp\\Desktop\\1.5相似完成\\parquet2")
      /*val df = sqlContext.read.parquet("C:\\Users\\ywp\\Desktop\\1.5相似完成\\parquet2")
      df.printSchema()*/

    }*/

  test("my result") {
    val sb = udf { (x: String) =>
      val s = x.trim.replaceAll(" ", "")
      if (s.isEmpty) "1"
      else {
        s match {
          case "0" | "0.0" => "0"
          case _ => "1"
        }
      }
    }

    /*import sqlContext.implicits._
    val csl = csvContext.enableHive
    val ds = ArrayBuffer[DataFrame]()
    Files.list(Paths.get("C:\\Users\\ywp\\Desktop\\标记为1但是不属于活跃品牌_且没有出现在本次活跃门店中\\csv"))
      .iterator()
      .foreach(p => {
        val fileName = p.getFileName.toString
        val code = fileName.substring(0, fileName.size - 4)
        val filePath = p.toString
        val df = csl.csvFile(
          filePath = filePath,
          charset = "UTF-8"
        ).withColumnRenamed("商户代码MCHNT_CD", "MCHNT_CD")
          //          .select('MCHNT_CD.as('src_shop_no), lit("1").as('shop_valid_st))
          .select('MCHNT_CD.as('src_shop_no), lit(code).as('file))
        ds.append(df)
      })

    Files.list(Paths.get("C:\\Users\\ywp\\Desktop\\活跃品牌截止到10.25\\csv"))
      .iterator()
      .foreach(p => {
        val fileName = p.getFileName.toString
        val code = fileName.substring(0, fileName.size - 4)
        val filePath = p.toString
        val df = csl.csvFile(
          filePath = filePath,
          charset = "UTF-8"
        ).withColumnRenamed("商户代码MCHNT_CD", "MCHNT_CD")
          //          .select('MCHNT_CD.as('src_shop_no), sb('相似).as('shop_valid_st))
          .select('MCHNT_CD.as('src_shop_no), lit(code).as('file))
        ds.append(df)
      })
    var sDF = ds.head
    for (x <- ds.tail) sDF = sDF.unionAll(x)
    val flatten = udf((xs: Seq[String]) => xs.mkString("|"))
    val x = sDF.groupBy('src_shop_no)
      //      .agg(collect_list('shop_valid_st).as('st_list))
      //      .filter("size(st_list)>1")
      //      .select('src_shop_no, flatten('st_list).as('st_list))
      .agg(collect_list('file).as('file_list))
      .filter("size(file_list)>1")
      .select('src_shop_no, flatten('file_list).as('file_list))

    writeTocsv("C:\\Users\\ywp\\Desktop", "dupIds", x, deli = ",")*/

    import sqlContext.implicits._
    val csl = csvContext.enableHive
    val ds = ArrayBuffer[DataFrame]()
    Files.list(Paths.get("C:\\Users\\ywp\\Desktop\\1.5相似完成\\csv"))
      .iterator()
      .foreach(p => {
        val fileName = p.getFileName.toString
        val code = fileName.substring(0, fileName.size - 4)
        val filePath = p.toString
        val df = csl.csvFile(
          filePath = filePath,
          charset = "UTF-8"
        )
        /* val fs = df.schema.fieldNames
         val head = fs.head
         val sim = fs(fs.size - 2)
         val last = fs.last
         println(code, head, sim, last)*/
        val xDF = reNameDF(df)
          .withColumnRenamed("商户代码MCHNT_CD", "MCHNT_CD")
          .withColumnRenamed("\uFEFF商户代码MCHNT_CD", "MCHNT_CD")
          .withColumnRenamed("相似商户名称", "相似商户代码")
          .withColumnRenamed("相似的商户代码", "相似商户代码")
          .withColumnRenamed("相似", "sim")
          .select('MCHNT_CD.as('src_shop_no), '相似商户代码.as('sim_no), sb('sim).as('sim))
        ds.append(xDF)
      })
    var sDF = ds.head
    for (x <- ds.tail) sDF = sDF.unionAll(x)
    //    val a = sDF.filter("sim='1'").select('src_shop_no, 'sim.as('st))
    val b = sDF.filter("sim='0'").select('sim_no.as('src_shop_no)).distinct().except(sDF.select('src_shop_no).distinct())

    val allDF = sqlContext.read.parquet("C:\\Users\\ywp\\Desktop\\1.5.1全量数据\\parquet")
      .withColumn("商户代码MCHNT_CD", trim('商户代码MCHNT_CD))
      .withColumnRenamed("品牌名BAND_NM", "品牌名BRAND_NM")

    val appendDF = allDF.as('a)
      .join(b.as('b), $"a.商户代码MCHNT_CD" === $"b.src_shop_no", "leftsemi")
      .selectExpr("a.*")
      .withColumn("商户名称MCHNT_NM", fuckName(caoName('商户名称MCHNT_NM, '商户名称), caoName('品牌名BRAND_NM, '品牌名)))

    writeTocsv("C:\\Users\\ywp\\Desktop", "not_exists_activity", appendDF)

    /* val xxDF = a.unionAll(b)

     val flatten = udf((xs: Seq[String]) => xs.mkString("|"))
     val x = xxDF.groupBy('src_shop_no)
       //      .agg(collect_list('shop_valid_st).as('st_list))
       //      .filter("size(st_list)>1")
       //      .select('src_shop_no, flatten('st_list).as('st_list))
       .agg(collect_list('st).as('st))
       .filter("size(st)>1")
       .select('src_shop_no, flatten('st).as('st))

     writeTocsv("C:\\Users\\ywp\\Desktop", "dupIds2", x, deli = ",")*/

  }

  test("fuck all") {
    val sb = udf { (x: String) =>
      val s = x.trim.replaceAll(" ", "")
      if (s.isEmpty) "1"
      else {
        s match {
          case "0" | "0.0" => "0"
          case _ => "1"
        }
      }
    }
    import sqlContext.implicits._
    val ds = ArrayBuffer[DataFrame]()
    val csl = csvContext.enableHive

    Files
      .list(Paths.get("C:\\Users\\ywp\\Desktop\\1.5相似完成\\csv"))
      .iterator()
      .map(_.toString)
      .foreach(x => {
        val df = csl.csvFile(
          filePath = x,
          charset = "UTF-8"
        )

        val xDF = reNameDF(df)
          .withColumnRenamed("\uFEFF商户代码MCHNT_CD", "MCHNT_CD")
          .withColumnRenamed("商户代码MCHNT_CD", "MCHNT_CD")
          .withColumn("st", sb('相似))
          .selectExpr("MCHNT_CD  src_shop_no", "st")
        ds.append(xDF)
      })

    var tmpDF = ds.head
    for (x <- ds.tail) tmpDF = tmpDF.unionAll(x)

    val xDF = tmpDF.groupBy('src_shop_no).agg(collect_list('st).as("sl"))
    val bDF = xDF.filter("size(sl)>1")
    writeTocsv("C:\\Users\\ywp\\Desktop", "多个状态的门店id", bDF, deli = ",")
    //    val mDF = xDF.filter("size(sl)=1").selectExpr("src_shop_no", "sl[0] st")

    //    val df = csl.csvFile(
    //      filePath = "C:\\Users\\ywp\\Desktop\\not_exists_activity.csv",
    //      charset = "UTF-8",
    //      delimiter = '\t'
    //    )
    //    val sDF = df
    //      .withColumnRenamed("商户代码MCHNT_CD", "MCHNT_CD")
    //      .withColumnRenamed("\uFEFF商户代码MCHNT_CD", "MCHNT_CD")
    //      .withColumn("MCHNT_CD", trim('MCHNT_CD))
    //      .selectExpr("MCHNT_CD  src_shop_no")
    //      .distinct()
    //      .withColumn("st", lit("1"))
    //    bDF.selectExpr("src_shop_no", "'1' st").unionAll(mDF).unionAll(sDF).repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ywp\\Desktop\\needId")

  }


  test("shop name") {
    import sqlContext.implicits._
    def appendDF(path: String): DataFrame = {
      val ds = ArrayBuffer[DataFrame]()

      sqlContext.udf.register("sim", (s: String) => {
        val ss = s.trim.replaceAll(" ", "")
        ss match {
          case "0" | "0.0" => "0"
          case _ => "1"
        }
      })

      sqlContext.udf.register("delName", (n: String) => {
        val nm = n.trim.replaceAll(" ", "")
        nm match {
          case "2" | "2.0" => ""
          case _ => nm
        }
      })

      sqlContext.udf.register("bn", (o: String, n: String) => {
        val on = o.trim.replaceAll(" ", "")
        val nn = n.trim.replaceAll(" ", "")
        if (nn.isEmpty) on else nn
      })

      val sn = udf((o: String, n: String, b: String) => {
        val on = o.trim.replaceAll(" ", "")
        val nn = n.trim.replaceAll(" ", "")
        if (nn.isEmpty) {
          if (on.contains(b))
            on
          else
            b + "(" + on + ")"
        } else nn
      })

      Files
        .list(Paths.get(path))
        .iterator()
        .map(_.toString)
        .foreach(x => {
          val df = csvContext.csvFile(
            filePath = x,
            charset = "UTF-8",
            useHeader = false
          )
          val fs = df.schema.fieldNames
          val shopNo = fs(0)
          val oldShopName = fs(1)
          val newShopName = fs(3)
          val oldAddr = fs(4)
          val newAddr = fs(5)
          val city = fs(9)
          val simNo = fs(64)
          val brandNo = fs(49)
          val newBrandName = fs(61)
          val oldBrandName = fs(60)
          val st = fs(63)
          val ex = Seq(s"trim($shopNo) shopNo", s"trim($oldShopName) oldShopName", s"delName($newShopName) newShopName",
            s"bn(trim($oldBrandName),trim($newBrandName)) brandName",
            s"bn(trim($oldAddr),trim($newAddr)) shopAddr", s"trim($city) city", s"sim($st) st")
          val xDF = df.selectExpr(ex: _*)
            .filter($"st" === 1)
            .select('shopNo, sn('oldShopName, 'newShopName, 'brandName).as('shopName), 'city, 'shopAddr)
          ds.append(xDF)
        })

      var tmpDF = ds.head
      for (x <- ds.tail) tmpDF = tmpDF.unionAll(x)
      tmpDF
    }

    val df1 = appendDF("C:\\Users\\ls\\Desktop\\Desktop\\1.6 截止10月25日活跃品牌门店 商户名称 品牌 相似 相似商户代码修改完成\\csv1128")
    val df2 = appendDF("C:\\Users\\ls\\Desktop\\Desktop\\1.5.2 被标记为1且不在活跃品牌门店数据\\csv1128")
    val df3 = appendDF("C:\\Users\\ls\\Desktop\\tmp\\csv1128")

    val df = df1.unionAll(df2).unionAll(df3).distinct()
    //      .groupBy("shopNo")
    //      .agg(first("shopName").as("shopName"), first("city").as("city"), first("shopAddr").as("shopAddr"))

    val pw = new PrintWriter("C:\\Users\\ls\\Desktop\\20161128\\shop.csv")
    pw.println("shopNo,shopName,cityCode,shopAddr")
    df.map(_.mkString(",")).collect().foreach(pw.println)
    pw.println()
    pw.close()

    //    df1.unionAll(df2).repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ls\\Desktop\\result_brand")
  }


  test("result") {
    import sqlContext.implicits._
    val df = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\result").select(trim('shopNo)).distinct()
    println(df.count())
  }

  test("result sim") {
    import sqlContext.implicits._
    val allDF = sqlContext.enableHive.read.parquet("C:\\Users\\ls\\Desktop\\result_new_1")
      .select(trim('shopNo).as('src_shop_no), trim('simi).as('sim), trim('simNo).as('sim_no))
    val m = allDF.map(r => {
      val so = r.getAs[String]("src_shop_no")
      val sim = r.getAs[String]("sim")
      (so, sim)
    }).collect().toMap

    val bm = sc.broadcast(m)

    val simDF = allDF.filter("sim='0'")
      .selectExpr("src_shop_no  shopNo", "sim_no otherNo")
    val v = simDF.selectExpr("shopNo id").unionAll(simDF.selectExpr("otherNo id")).distinct().orderBy("id")
    val e = simDF.selectExpr("shopNo src", "otherNo dst").withColumn("relationship", lit("sim"))
    val g = GraphFrame(v, e)
    val result = g.connectedComponents.run()
    val gDF = result.groupBy("component")
      .agg(collect_set("id") as ("ids"))
      .selectExpr("ids")
      .mapPartitions(it => {
        val mp = bm.value
        it.map(row => {
          val ids = row.getAs[Seq[String]]("ids")
          val tp = ids.map(id => (id, mp.getOrElse(id, "1")))
          val (vL, nL) = tp.partition(_._2 == "1")
          Option(nL) match {
            case None => ("", Seq.empty[String])
            case Some(nnL) => {
              if (nnL.isEmpty) ("", Seq.empty[String])
              else {
                if (vL.isEmpty) ("", Seq.empty[String]) else (vL.head._1, nnL.map(_._1))
              }
            }
          }
        })
      })
      .toDF("src_shop_no", "b_list").filter("src_shop_no!=''")
    gDF.repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ls\\Desktop\\result_sim_graph")
    /*val df = sqlContext.read.parquet("C:\\Users\\ywp\\Desktop\\result_sim_graph")
    df.printSchema()
    df.map(_.mkString(",")).collect().foreach(println)
    println(df.count())*/
  }

  test("tmp") {
    import sqlContext.implicits._
    sqlContext.udf.register("delName", (n: String) => {
      val nm = n.trim.replaceAll(" ", "")
      nm match {
        case "2" | "2.0" => ""
        case _ => nm
      }
    })

    sqlContext.udf.register("bn", (o: String, n: String) => {
      val on = o.trim.replaceAll(" ", "")
      val nn = n.trim.replaceAll(" ", "")
      if (nn.isEmpty) on else nn
    })

    sqlContext.udf.register("st", (st: String) => {
      val res = st.trim.replaceAll(" ", "")
      res match {
        case "0" | "0.0" => "0"
        case _ => "1"
      }
    })

    val sn = udf((o: String, n: String, b: String) => {
      val on = o.trim.replaceAll(" ", "")
      val nn = n.trim.replaceAll(" ", "")
      if (nn.isEmpty) {
        if (on.contains(b))
          on
        else
          b + "(" + on + ")"
      } else nn
    })

    val df = csvContext.csvFile(
      filePath = "C:\\Users\\ls\\Desktop\\tmp\\复核后修改.csv",
      charset = "UTF-8",
      useHeader = false
    )

    val fs = df.schema.fieldNames
    val shopNo = fs(0)
    val oldShopName = fs(1)
    val newShopName = fs(3)
    val oldAddr = fs(4)
    val newAddr = fs(5)
    val oldBrandName = fs(60)
    val newBrandName = fs(61)
    val st = fs(63)
    val ex = Seq(s"trim($shopNo) src_shop_no", s"trim($oldShopName) oldShopName", s"delName($newShopName) newShopName",
      s"bn($oldBrandName,$newBrandName) brandName", s"bn($oldAddr,$newAddr) shop_addr", s"st($st) shop_valid_st")
    val ds = df.selectExpr(ex: _*)
      .select('src_shop_no, sn('oldShopName, 'newShopName, 'brandName).as('shop_nm), 'shop_addr, 'shop_valid_st)
      .groupBy("src_shop_no")
      .count()
      .filter("count > 1")
    ds.show()
    //        val s = r.getAs[String]("src_shop_no")
    //        val n = r.getAs[String]("shop_nm").replace("\'", "\\'")
    //        val st = r.getAs[String]("shop_valid_st")
    //        val addr = r.getAs[String]("shop_addr").replace("\'", "\\'")
    //        s"update tbl_content_shop_inf set shop_nm='$n',shop_valid_st='$st',shop_addr='$addr' where src_shop_no='$s';"
    //      }).collect()
    //    def writeSql(name: String, ds: Seq[String]) = {
    //      val pw = new PrintWriter(s"C:\\Users\\ls\\Desktop\\Desktop\\sql\\$name.sql")
    //      ds.foreach(pw.println)
    //      pw.close()
    //    }
    //    writeSql("复核后修改", ds)
  }

  test("delShopName") {
    import sqlContext.implicits._
    sqlContext.udf.register("delName", (n: String) => {
      val nm = n.trim.replaceAll(" ", "")
      nm match {
        case "2" | "2.0" => ""
        case _ => nm
      }
    })

    sqlContext.udf.register("bn", (o: String, n: String) => {
      val on = o.trim.replaceAll(" ", "")
      val nn = n.trim.replaceAll(" ", "")
      if (nn.isEmpty) on else nn
    })

    sqlContext.udf.register("st", (st: String) => {
      val res = st.trim.replaceAll(" ", "")
      res match {
        case "0" | "0.0" => "0"
        case _ => "1"
      }
    })

    val sn = udf((o: String, n: String, b: String) => {
      val on = o.trim.replaceAll(" ", "")
      val nn = n.trim.replaceAll(" ", "")
      if (nn.isEmpty) {
        if (on.contains(b))
          on
        else
          b + "(" + on + ")"
      } else nn
    })


    val dir = new File("C:\\Users\\ls\\Desktop\\Desktop\\1.6 截止10月25日活跃品牌门店 商户名称 品牌 相似 相似商户代码修改完成\\csv1128")
    val children = dir.listFiles.filter(_.isFile)

    children.foreach(f => {
      println(f.getAbsolutePath)
      val df = csvContext.csvFile(
        f.getAbsolutePath,
        charset = "UTF-8",
        useHeader = true
      )
      val fs = df.schema.fieldNames
      val shopNo = fs(0)
      val oldShopName = fs(1)
      val newShopName = fs(3)
      val oldBrandName = fs(60)
      val newBrandName = fs(61)
      val st = fs(63)
      val ex = Seq(s"trim($shopNo) src_shop_no", s"trim($oldShopName) oldShopName", s"delName($newShopName) newShopName",
        s"bn($oldBrandName,$newBrandName) brandName", s"st($st) shop_valid_st")
      val ds = df.selectExpr(ex: _*)
        .select('src_shop_no, sn('oldShopName, 'newShopName, 'brandName).as('shop_nm), 'shop_valid_st)
        .map(r => {
          val s = r.getAs[String]("src_shop_no")
          val n = r.getAs[String]("shop_nm").replaceAll("\'", "\\'")
          val st = r.getAs[String]("shop_valid_st")
          s"update tbl_content_shop_inf set shop_nm='$n',shop_valid_st='$st' where src_shop_no='$s';"
        }).collect()
      def writeSql(name: String, ds: Seq[String]) = {
        val pw = new PrintWriter(s"C:\\Users\\ls\\Desktop\\Desktop\\sql\\$name.sql")
        ds.foreach(pw.println)
        pw.close()
      }
      writeSql(f.getName, ds)
    })
  }

  test("union") {
    import sqlContext.implicits._
    val allDF = sqlContext.enableHive.read.parquet("C:\\Users\\ls\\Desktop\\result")
    sqlContext.udf.register("sim", (s: String) => {
      val ss = s.trim.replaceAll(" ", "")
      ss match {
        case "0" | "0.0" => "0"
        case _ => "1"
      }
    })

    sqlContext.udf.register("delName", (n: String) => {
      val nm = n.trim.replaceAll(" ", "")
      nm match {
        case "2" | "2.0" => ""
        case _ => nm
      }
    })

    val addDF = csvContext.csvFile(
      filePath = "C:\\Users\\ls\\Desktop\\tmp\\复核后修改.csv",
      charset = "UTF-8",
      useHeader = true
    )
    val fs = addDF.schema.fieldNames
    val shopNo = fs(0)
    val oldShopName = fs(1)
    val newShopName = fs(3)
    val simNo = fs(64)
    val sm = fs(63)
    val newBrandName = fs(61)
    val oldBrandName = fs(60)
    val ex = Seq(s"trim($shopNo) shopNo", s"trim($oldShopName) oldShopName", s"delName($newShopName) newShopName",
      s"trim($oldBrandName) oldBrandName", s"trim($newBrandName) newBrandName", s"sim($sm) simi", s"$simNo simNo")

    val xDF = addDF.selectExpr(ex: _*)

    val tDF = xDF.selectExpr("shopNo").map(_.getAs[String]("shopNo")).collect()

    val tmpDF = allDF.filter(!($"shopNo".isin(tDF: _*)))
    val finalDF = tmpDF.unionAll(xDF)

    //    val tmpDF = allDF.except(xDF)
    //
    //    val finalDF = tmpDF.unionAll(xDF)
    //
    finalDF.repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ls\\Desktop\\result_new_1")
  }



  test("增量数据") {
    import sqlContext.implicits._
    def appendDF(path: String): DataFrame = {
      val ds = ArrayBuffer[DataFrame]()

      Files
        .list(Paths.get(path))
        .iterator()
        .map(_.toString)
        .foreach(x => {
          val df = csvContext.csvFile(
            filePath = x,
            charset = "UTF-8",
            useHeader = true
          )
          ds.append(df)
        })

      var tmpDF = ds.head
      for (x <- ds.tail) tmpDF = tmpDF.unionAll(x)
      tmpDF
    }

    val df = appendDF("C:\\Users\\ls\\Desktop\\11.29\\1025-1111门店数据\\csv1128")

    df.repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ls\\Desktop\\11.29\\result")
  }

  test("test result") {
    import sqlContext.implicits._
    val df = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\11.29\\result_tag")
      .selectExpr("MCHNT_CD", "simi", "simId")
      .filter($"MCHNT_CD" === "T00000000491098")

    df.show()

    //    df.repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ls\\Desktop\\11.29\\result1129")

    //    df.show(2)
  }

  test("brand") {
    import sqlContext.implicits._
    def appendDF(path: String): DataFrame = {
      val ds = ArrayBuffer[DataFrame]()

      sqlContext.udf.register("bn", (o: String, n: String) => {
        val on = o.trim.replaceAll(" ", "")
        val nn = n.trim.replaceAll(" ", "")
        if (nn.isEmpty) on else nn
      })

      Files
        .list(Paths.get(path))
        .iterator()
        .map(_.toString)
        .foreach(x => {
          val df = csvContext.csvFile(
            filePath = x,
            charset = "UTF-8",
            useHeader = true
          )
          val fs = df.schema.fieldNames
          val brandNo = fs(50)
          val newBrandName = fs(62)
          val oldBrandName = fs(61)
          val ex = Seq(s"trim($brandNo) brandNo", s"bn(trim($oldBrandName),trim($newBrandName)) brandName")
          val xDF = df.selectExpr(ex: _*)
          ds.append(xDF)
        })

      var tmpDF = ds.head
      for (x <- ds.tail) tmpDF = tmpDF.unionAll(x)
      tmpDF
    }

    val df = appendDF("C:\\Users\\ls\\Desktop\\11.29\\1025-1111门店数据\\csv1128")
      .distinct()

    df.show(20)

    df.repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ls\\Desktop\\11.29\\brand")
  }

  test("test") {
    val df = csvContext.csvFile(
      filePath = "C:\\Users\\ls\\Desktop\\1.csv",
      charset = "UTF-8",
      useHeader = true
    ).unionAll(csvContext.csvFile(
      filePath = "C:\\Users\\ls\\Desktop\\2.csv",
      charset = "UTF-8",
      useHeader = true
    ))

    df.show()
  }

  test("simShop") {
    import sqlContext.implicits._
    val shopDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\数据\\上线后数据备份\\上线数据\\处理前数据\\exportData\\data1202\\tbl_content_shop_inf30")

    val pw = new PrintWriter("C:\\Users\\ls\\Desktop\\20161202\\delData.txt")

    pw.println("src_shop_no\tshop_nm\tshop_addr\tshop_lnt\tshop_lat\tshop_contact_phone")

    val tmpDF = shopDF
      .selectExpr("src_shop_no", "shop_nm", "shop_addr", "shop_lnt", "shop_lat", "shop_contact_phone")
      .filter($"shop_contact_phone".contains("."))

    println(tmpDF.count())

    tmpDF.map(_.mkString("\t"))
      .collect()
      .foreach(pw.println)

    pw.println()

    pw.close()
    //    val shopUnionDF = shopDF.filter($"src_shop_tp" === 1 and ($"shop_valid_st" === 0))
    //      .selectExpr("src_shop_no", "shop_nm", "county_cd", "shop_addr", "industry_no")
    //
    //    val shopCrawlDF = shopDF.filter($"src_shop_tp" === 0 and ($"shop_valid_st" === 0))
    //      .selectExpr("src_shop_no", "shop_nm", "county_cd", "shop_addr", "industry_no")
    //
    //    println(shopDF.count())
    //    println(shopUnionDF.count())
    //    println(shopCrawlDF.count())
    //
    //    shopUnionDF.repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ls\\Desktop\\20161202\\unionShop")
    //    shopCrawlDF.repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ls\\Desktop\\20161202\\crawlShop")

  }


  test("12012") {
    import sqlContext.implicits._
    def appendDF(path: String): DataFrame = {
      val ds = ArrayBuffer[DataFrame]()

      sqlContext.udf.register("sim", (s: String) => {
        val ss = s.trim.replaceAll(" ", "")
        ss match {
          case "0" | "0.0" => "0"
          case _ => "1"
        }
      })

      Files
        .list(Paths.get(path))
        .iterator()
        .map(_.toString)
        .foreach(x => {
          val df = csvContext.csvFile(
            filePath = x,
            charset = "UTF-8",
            useHeader = true
          )
          val fs = df.schema.fieldNames
          val shopNo = fs(0)
          val simNo = fs(64)
          val sm = fs(63)
          val ex = Seq(s"trim($shopNo) shopNo", s"sim($sm) simi", s"$simNo simNo")
          val xDF = df.selectExpr(ex: _*)
          ds.append(xDF)
        })

      var tmpDF = ds.head
      for (x <- ds.tail) tmpDF = tmpDF.unionAll(x)
      tmpDF
    }

    val df1 = appendDF("C:\\Users\\ls\\Desktop\\Desktop\\1.5.2 被标记为1且不在活跃品牌门店数据\\csv1128")
    val df2 = appendDF("C:\\Users\\ls\\Desktop\\Desktop\\1.6 截止10月25日活跃品牌门店 商户名称 品牌 相似 相似商户代码修改完成\\csv1128")
    df1.unionAll(df2).repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ls\\Desktop\\20161202\\result")

  }

  test("tmp1202") {
    import sqlContext.implicits._
    def appendDF(path: String): DataFrame = {
      val ds = ArrayBuffer[DataFrame]()

      sqlContext.udf.register("sim", (s: String) => {
        val ss = s.trim.replaceAll(" ", "")
        ss match {
          case "0" | "0.0" => "0"
          case _ => "1"
        }
      })

      Files
        .list(Paths.get(path))
        .iterator()
        .map(_.toString)
        .foreach(x => {
          val df = csvContext.csvFile(
            filePath = x,
            charset = "UTF-8",
            useHeader = true
          )
          val fs = df.schema.fieldNames
          val shopNo = fs(0)
          val simNo = fs(64)
          val sm = fs(63)
          val ex = Seq(s"trim($shopNo) shopNo", s"sim($sm) simi", s"$simNo simNo")
          val xDF = df.selectExpr(ex: _*)
          ds.append(xDF)
        })

      var tmpDF = ds.head
      for (x <- ds.tail) tmpDF = tmpDF.unionAll(x)
      tmpDF
    }

    val df1 = appendDF("C:\\Users\\ls\\Desktop\\tmp\\csv1128")
    df1.repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ls\\Desktop\\20161202\\tmp")
  }

  test("distinct1202") {
    import sqlContext.implicits._
    val allDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\20161202\\result")

    val xDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\20161202\\tmp")

    val ids = xDF.selectExpr("shopNo").map(_.getAs[String]("shopNo")).collect()

    val tmpDF = allDF.filter(!($"shopNo".isin(ids: _*)))
    val finalDF = tmpDF.unionAll(xDF)

    finalDF.printSchema()
    println(finalDF.count())

    val df = finalDF.groupBy("shopNo")
      .agg(first("simi").as("simi"), first("simNo").as("simNo"))

    println(df.count())

    df.repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ls\\Desktop\\20161202\\update_result")
  }


  test("sim1202") {
    import sqlContext.implicits._
    val allDF = sqlContext.enableHive.read.parquet("C:\\Users\\ls\\Desktop\\20161202\\update_result")
      .select(trim('shopNo).as('src_shop_no), trim('simi).as('sim), trim('simNo).as('sim_no))
    val m = allDF.map(r => {
      val so = r.getAs[String]("src_shop_no")
      val sim = r.getAs[String]("sim")
      (so, sim)
    }).collect().toMap

    val bm = sc.broadcast(m)

    val simDF = allDF.filter("sim='0'")
      .selectExpr("src_shop_no  shopNo", "sim_no otherNo")
    val v = simDF.selectExpr("shopNo id").unionAll(simDF.selectExpr("otherNo id")).distinct().orderBy("id")
    val e = simDF.selectExpr("shopNo src", "otherNo dst").withColumn("relationship", lit("sim"))
    val g = GraphFrame(v, e)
    val result = g.connectedComponents.run()
    val gDF = result.groupBy("component")
      .agg(collect_set("id") as ("ids"))
      .selectExpr("ids")
      .mapPartitions(it => {
        val mp = bm.value
        it.map(row => {
          val ids = row.getAs[Seq[String]]("ids")
          val tp = ids.map(id => (id, mp.getOrElse(id, "1")))
          val (vL, nL) = tp.partition(_._2 == "1")
          Option(nL) match {
            case None => ("", Seq.empty[String])
            case Some(nnL) => {
              if (nnL.isEmpty) ("", Seq.empty[String])
              else {
                if (vL.isEmpty) ("", Seq.empty[String]) else (vL.head._1, nnL.map(_._1))
              }
            }
          }
        })
      })
      .toDF("src_shop_no", "b_list").filter("src_shop_no!=''")
    gDF.repartition(4).write.mode(SaveMode.Overwrite).parquet("C:\\Users\\ls\\Desktop\\20161202\\simi_result")
  }

  test("filter sim") {
    import sqlContext.implicits._

    val shopDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\20161202\\simi_result")
      .filter($"src_shop_no" === "T00000000282855")
      .selectExpr("explode(b_list) src_shop_no")

    println(shopDF.count())
    shopDF.show(1000000)


  }



  test("relation") {
    import sqlContext.implicits._

    val df = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\20161202\\relation")
      .filter($"shop_valid_st" === "0")
      .filter($"relation_src_shop_no" !== "")
      .selectExpr("src_shop_no", "relation_src_shop_no")

    val ds = df.map(r => {
      val s = r.getAs[String]("src_shop_no")
      val l = r.getAs[String]("relation_src_shop_no")
      s"update tbl_content_shop_inf set relation_src_shop_no='$l' where src_shop_no='$s';"
    }).collect()
    def writeSql(name: String, ds: Seq[String]) = {
      val pw = new PrintWriter(s"C:\\Users\\ls\\Desktop\\20161202\\$name.sql")
      ds.foreach(pw.println)
      pw.close()
    }
    writeSql("relation_shop_tmp", ds)
    //
    //    val df1 = df.filter($"shop_valid_st" === "1" and ($"relation_src_shop_no" === ""))
    //    val df0 = df.filter($"shop_valid_st" === "0" and ($"relation_src_shop_no" === ""))
    //
    //    println(df1.count())
    //    println(df0.count())

  }

  test("验证") {
    import sqlContext.implicits._
    val shopDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\数据\\上线后数据备份\\上线数据\\处理前数据\\exportData\\data1202\\tbl_content_online_shop30")
      .filter($"src_shop_no" === "T00000000207793")

    shopDF.show()
  }

  test("删除") {
    import sqlContext.implicits._
    def appendDF(path: String): DataFrame = {
      val ds = ArrayBuffer[DataFrame]()

      Files
        .list(Paths.get(path))
        .iterator()
        .map(_.toString)
        .foreach(x => {
          val df = csvContext.csvFile(
            filePath = x,
            charset = "UTF-8",
            useHeader = false
          )
          val fs = df.schema.fieldNames
          val shopNo = fs(0)
          val brandNo = fs(49)
          val newBrandName = fs(61)
          val ex = Seq(s"trim($shopNo) shopNo", s"trim($newBrandName) brandName", s"trim($brandNo) brandNo")
          val xDF = df.selectExpr(ex: _*)
          ds.append(xDF)
        })

      var tmpDF = ds.head
      for (x <- ds.tail) tmpDF = tmpDF.unionAll(x)
      tmpDF
    }

    val df1 = appendDF("C:\\Users\\ls\\Desktop\\Desktop\\1.6 截止10月25日活跃品牌门店 商户名称 品牌 相似 相似商户代码修改完成\\csv1128")
    val df2 = appendDF("C:\\Users\\ls\\Desktop\\Desktop\\1.5.2 被标记为1且不在活跃品牌门店数据\\csv1128")
    val df3 = appendDF("C:\\Users\\ls\\Desktop\\tmp\\csv1128")

    val df = df1.unionAll(df2)

    val tmpDF3 = df3.select("shopNo").map(_.getAs[String]("shopNo")).collect()

    val allDF = df.filter(!$"shopNo".isin(tmpDF3: _*)).unionAll(df3)
      .selectExpr("brandName", "brandNo")
      .distinct()
      .filter($"brandName" !== "")

    val brandDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\数据\\上线后数据备份\\上线数据\\处理前数据\\exportData\\data1202\\tbl_content_brand_inf30")
      .filter($"src_brand_tp" === "1")
      .selectExpr("src_brand_no", "brand_nm")
      .filter(length($"src_brand_no") > 16)

    val resDF = brandDF.as('a)
      .join(allDF.as('b), $"a.brand_nm" === $"b.brandName", "left_outer")
      .selectExpr("a.src_brand_no", "coalesce(b.brandNo,'') brandNo")
      .filter($"brandNo" !== "")

    resDF.printSchema()
    println(resDF.count())
    resDF.show(100)
  }

  test("导出生产数据") {
    import sqlContext.implicits._

    val shop = new PrintWriter("C:\\Users\\ls\\Desktop\\20161206\\shop.csv")
    val shopDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\20161206\\tbl_content_shop_inf1205")
      .selectExpr("shop_no", "src_shop_no", "shop_nm", "shop_addr", "brand_no",
        "shop_valid_st", "shop_valid_dt_st", "shop_valid_dt_end")

    val brand = new PrintWriter("C:\\Users\\ls\\Desktop\\20161206\\brand.csv")

  }

  test("修复自有品牌id") {
    import sqlContext.implicits._

    val seq = Seq("T00000000366711", "T00000000291859", "T00000000292038")
    val shopOriginDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\20161206\\tbl_chmgm_preferential_mchnt_inf1206")
      .filter($"MCHNT_CD".isin(seq: _*))
    //      .selectExpr("MCHNT_CD", "BRAND_ID")
    shopOriginDF.show()
    /*val shopDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\20161206\\tbl_content_shop_inf1205")
      .filter($"src_shop_tp" === 1)
      .selectExpr("src_shop_no", "brand_no")

    println(shopDF.count())


    val brandDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\20161206\\tbl_content_brand_inf1205")
      .filter($"src_brand_tp" === 1)
      .selectExpr("brand_no", "src_brand_no")
      .filter("length(src_brand_no) > 16")


    val tmpDF = shopDF.as('a)
      .join(shopOriginDF.as('b), $"a.src_shop_no" === $"b.MCHNT_CD", "left_outer")
      .selectExpr("a.*", "coalesce(b.BRAND_ID,'') src_brand_no")
    println(tmpDF.count())


    val finalDF = tmpDF.as('a)
      .join(brandDF.as('b), $"a.brand_no" === $"b.brand_no", "left_outer")
      .selectExpr("a.src_shop_no", "a.brand_no", "a.src_brand_no oldBrandNo", "coalesce(b.src_brand_no,'') newBrandNo")

    println(finalDF.count())

    println(finalDF.filter($"newBrandNo" === "").count())*/

  }


  test("删除多余品牌") {
    import sqlContext.implicits._

    val shopDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\export\\20161207\\tbl_content_shop_inf1206")
      .selectExpr("src_shop_no", "shop_valid_st", "brand_no")

    val shopAllDF = shopDF.selectExpr("src_shop_no", "brand_no")
      .groupBy("brand_no").agg(countDistinct("src_shop_no").as("shopCountAll"))

    val shopDF1 = shopDF.filter($"shop_valid_st" === "1")
      .selectExpr("src_shop_no", "brand_no")
      .groupBy("brand_no").agg(countDistinct("src_shop_no").as("shopCountSt1"))

    val brandDF1 = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\export\\20161207\\tbl_content_brand_inf")
      .filter(!$"brand_desc".contains("insert"))
      .selectExpr("brand_no", "src_brand_tp", "brand_nm", "brand_intro", "brand_desc")
      .map(r => {
        val brand_no = r.getAs[String]("brand_no")
        val src_brand_tp = r.getAs[String]("src_brand_tp")
        val brand_intro = r.getAs[String]("brand_intro")
        val brand_desc = r.getAs[String]("brand_desc").replaceAll("[\r|\n|\r\n|\\s+|\\t+]", "")
        (brand_no, src_brand_tp, brand_intro, brand_desc)
      }).toDF("brand_no", "src_brand_tp", "brand_intro", "brand_desc")

    val brandDF2 = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\export\\20161207\\23条数据")
      .drop("src_brand_no")
      .map(r => {
        val brand_no = r.getAs[String]("brand_no")
        val src_brand_tp = r.getAs[String]("src_brand_tp")
        val brand_intro = r.getAs[String]("brand_intro")
        val brand_desc = r.getAs[String]("brand_desc").replaceAll("[\r|\n|\r\n|\\s+|\\t+]", "")
        (brand_no, src_brand_tp, brand_intro, brand_desc)
      }).toDF("brand_no", "src_brand_tp", "brand_intro", "brand_desc")

    val brandDF = brandDF1.unionAll(brandDF2)

    val pw = new PrintWriter("C:\\Users\\ls\\Desktop\\export\\20161207\\brand_new.txt")
    pw.println("brand_no(品牌id),src_brand_tp(品牌来源),brand_nm(品牌名)," +
      "brand_intro(品牌简介),brand_desc(品牌介绍),shopCountAll(关联所有店铺数目),shopCountSt1(关联有效店铺数目),brand_valid_st(是否有效),relation_brand_no(相关联brand_no)")

    val df = brandDF.as('a)
      .join(shopAllDF.as('b), $"a.brand_no" === $"b.brand_no", "left_outer")
      .join(shopDF1.as('c), $"a.brand_no" === $"c.brand_no", "left_outer")
      .selectExpr("a.*", "coalesce(b.shopCountAll,'0') shopCountAll", "coalesce(c.shopCountSt1,'0') shopCountSt1")

    df.map(_.mkString(",")).collect.foreach(pw.println)
    pw.close()

  }

  test("landMark") {
    import sqlContext.implicits._

    val df = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\20161212\\landMark")
      .selectExpr("landmark_no", "landmark_nm", "landmark_lnt", "landmark_lat", "prov_cd", "city_cd", "county_cd")

    df.show(5)
    val pw = new PrintWriter("C:\\Users\\ls\\Desktop\\20161212\\land.sql", "UTF-8")

    pw.println("INSERT INTO `tbl_content_landmark_inf` (`landmark_no`, `landmark_nm`,`landmark_lnt`, `landmark_lat`,`prov_cd`, `city_cd`,`county_cd`) VALUES")
    df.map(r => {
      val landmark_no = r.getAs[String]("landmark_no")
      val landmark_nm = r.getAs[String]("landmark_nm")
      val landmark_lnt = r.getAs[Double]("landmark_lnt")
      val landmark_lat = r.getAs[Double]("landmark_lat")
      val prov_cd = r.getAs[String]("prov_cd")
      val city_cd = r.getAs[String]("city_cd")
      val county_cd = r.getAs[String]("county_cd")
      s"('$landmark_no','$landmark_nm','$landmark_lnt','$landmark_lat','$prov_cd','$city_cd','$county_cd'),"
    }).collect().foreach(pw.println)
    pw.close()
  }

  test("write to mysql") {
    import sqlContext.implicits._

    val shopDF = sqlContext.read.parquet("C:\\Users\\ls\\Desktop\\数据\\上线后数据备份\\上线数据\\处理前数据\\exportData\\data1202\\tbl_content_shop_inf30")

    shopDF.printSchema()
  }

}








