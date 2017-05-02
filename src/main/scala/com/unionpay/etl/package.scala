package com.unionpay

import java.sql.Timestamp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.joda.time.LocalDateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable.ArrayBuffer

/**
  * Created by ywp on 2016/7/13.
  */
package object etl {


  private lazy val timeStampFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  private lazy val currentTimestamp = Timestamp.valueOf(LocalDateTime.now().toString(timeStampFormatter))

  implicit class AppendColumn(df: DataFrame) {

    /**
      * 每张表需要增加的默认值
      *
      * @return new dataFrame
      */
    def addAlwaysColumn: DataFrame = {
      df.withColumn("ROW_CRT_USR", lit(""))
        .withColumn("REC_UPD_USR", lit(""))
        .withColumn("ROW_CRT_TS", lit(currentTimestamp))
        .withColumn("REC_UPD_TS", lit(currentTimestamp))
    }

    /**
      * 需要设置的默认值
      *
      * @param defaultMap
      * @return
      */
    def appendDefaultVColumn(defaultMap: Map[String, Any]): DataFrame = {
      var tmpDF = df
      for ((k, v) <- defaultMap) {
        tmpDF = tmpDF.withColumn(k, lit(v))
      }
      tmpDF
    }

    /**
      * 字段重新命名
      *
      * @param transformMap
      * @return
      */

    def reNameColumn(transformMap: Map[String, String]): DataFrame = {
      var tmpDF = df
      for ((oldName, newName) <- transformMap) {
        tmpDF = tmpDF.withColumnRenamed(oldName, newName)
      }
      tmpDF
    }


    /**
      * mongo connector spark bug fixed
      * array [] column not exists
      *
      * @param collection
      * @return
      */
    def fixNotExistColumnException(collection: String): DataFrame = {
      val fields = df.schema.fieldNames
      collection.toLowerCase match {
        case "coupon" => {
          val buffer = ArrayBuffer[String](fields: _*)
          if (!fields.contains("pictureList")) {
            buffer.append("array('') pictureList")
          }

          if (!fields.contains("limitDetails")) {
            buffer.append("array('') limitDetails")
          }
          df.selectExpr(buffer: _*)
        }
        case "shop" => {
          var tmp = df
          //"coordinates",
          val checkNames = Array("tags", "pictureList")
          for (cn <- checkNames) {
            cn match {
              //              case "coordinates" if !fields.contains(cn) => tmp = tmp.selectExpr("*", "array(0,0) coordinates")
              case "tags" if !fields.contains(cn) => tmp = tmp.selectExpr("*", "array('') tags")
              case "pictureList" if !fields.contains(cn) => tmp = tmp.selectExpr("*", "array('') pictureList")
            }
          }
          tmp
        }
      }
    }

    /**
      * 仅针对Shop表 openingHours [] 时的处理
      *
      * @return
      */
    def dealOpeningHours: DataFrame = {
      val schema = df.schema
      val columns = ArrayBuffer(schema.fieldNames).flatten
      val excludOp = columns.filterNot(_ == "openingHours")
      //如果mongo中有一条数据具有openingHours完整的属性，则schema是完整的
      if (schema.toString().contains("start") && schema.toString().contains("end")) {
        excludOp append "case when size(openingHours)=0 then '' else concat_ws('-',date_format(openingHours[0].`start`,'H:mm'),date_format(openingHours[0].`end`,'H:mm')) end busi_time"
        df.selectExpr(excludOp: _*)
      } else {
        excludOp append "'' as busi_time"
        df.selectExpr(excludOp: _*)
      }

    }

    /**
      * shopIdList(onlineShopIdList offlineShopIdList合并) 处理
      *
      * @return
      */
    def dealShopList(implicit sqlContext: SQLContext): DataFrame = {
      sqlContext.udf.register("convertShopIdList", (shopIdList: Seq[String]) => shopIdList match {
        case null => Seq("")
        case _ => if (shopIdList.isEmpty) Seq("") else shopIdList
      })
      val schema = df.schema
      val columns = ArrayBuffer(schema.fieldNames).flatten
      if (schema.toString().contains("shopIdList")) {
        columns append "explode(convertShopIdList(shopIdList)) as src_shop_no"
        df.selectExpr(columns.filter(_ != "shopIdList"): _*)
      } else {
        columns append "'' as src_shop_no"
        df.selectExpr(columns: _*)
      }

    }

    /**
      * 数据库存储timestamp无法被java识别处理
      *
      * @return
      */
    def dealTimeStamp: DataFrame = {
      val allNameTypes = df.schema.fields.map(s => (s.name, s.dataType.typeName))
      val expres = allNameTypes.toSeq.map {
        case (name, ty) => {
          ty == "timestamp" match {
            case true => s"case when date_format($name,'yyyy')<'2000' then current_timestamp() else $name end $name"
            case false => name
          }
        }
      }
      df.selectExpr(expres: _*)
    }

  }


}
