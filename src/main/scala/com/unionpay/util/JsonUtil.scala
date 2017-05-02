package com.unionpay.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._

/**
  * Created by ywp on 2016/7/1.
  */
object JsonUtil {
  //jackson方式
  @transient lazy val mapper = new ObjectMapper()
  //    mapper.setSerializationInclusion(Include.NON_EMPTY)
  mapper.registerModule(DefaultScalaModule)
  //json4s方式
  implicit lazy val formats = DefaultFormats

  def toJacksonString(o: Object) = {
    mapper.writeValueAsString(o)
  }

  def toJacksonPrettyString(o: Object) = {
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(o)
  }

  def toJson4sString[T <: AnyRef](o: T) = {
    write[T](o)
  }

  def toJson4sPrettyString[T <: AnyRef](o: T) = {
    writePretty[T](o)
  }

  def readAsBeanByJackson[T](jsonStr: String)(implicit clazz: Class[T]) = {
    mapper.readValue(jsonStr, clazz)
  }


  def readAsBeanByJson4s[T: Manifest](jsonStr: String) = {
    read[T](jsonStr)
  }

}
