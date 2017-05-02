package com.unionpay.es

import com.unionpay.util.ConfigUtil
import org.apache.spark.SparkConf

/**
  * Created by ywp on 2016/7/27.
  */
package object es {


  import net.ceedubs.ficus.Ficus._
  import net.ceedubs.ficus.readers.ArbitraryTypeReader._

  private lazy val config = ConfigUtil.readClassPathConfig[EsConf]("es", "config")

  implicit class EsConfig(conf: SparkConf) {

    def build(index: String, typeTable: String, mapId: Option[String] = None): SparkConf = {
      val sc = conf
        //        .set("spark.buffer.pageSize", "8m")
        .set("es.nodes", config.nodes)
        .set("es.port", config.port)
        .set("es.scroll.size", "2000")
        .set("es.resource", s"$index/$typeTable")
        .set("es.index.auto.create", "true")
        .set("es.write.operation", "upsert")
      mapId match {
        case Some(id) => sc.set("es.mapping.id", id)
        case None => sc
      }
    }

  }

}

case class EsConf(nodes: String, port: String)
