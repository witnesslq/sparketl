package com.unionpay.test.ls

import java.sql.{Connection, PreparedStatement}
import com.unionpay.db.jdbc.JdbcUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.unionpay.db.jdbc.JdbcUtil._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by ls on 2016/11/5.
  */
object DealDb2Brand {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("DealDb2Brand === 删除Brand中的数据")
      //      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "32")

    val brandTmpDF = JdbcUtil.mysqlJdbcDF("tbl_chmgm_brand_tmp", "sink")
      .selectExpr("BRAND_ID")

    brandTmpDF.foreachPartition(it => {
      var connection: Connection = null
      var ps: PreparedStatement = null
      try {
        connection = getMysqlJdbcConnection(rootNode = "source")
        val ids = it.map(_.getAs[String]("BRAND_ID"))
        val sql = s"delete from tbl_chmgm_brand_inf where BRAND_ID in ${ids.map(x => s"'$x'").mkString("(", ",", ")")}"
        ps = connection.prepareStatement(sql)
        ps.execute()
      } catch {
        case e: Exception => throw e
      } finally {
        if (ps != null)
          ps.close
        if (connection != null)
          connection.close
      }
    })
    sc.stop()
  }
}
