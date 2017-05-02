package com.unionpay.util

import org.apache.spark.sql.{DataFrame, SQLContext}
import com.unionpay.db.jdbc.JdbcUtil

/**
  * Created by ywp on 2016/7/12.
  */
object UDF {


  implicit class udfClazz(@transient sqlContext: SQLContext) {

    def fetchIndustry: DataFrame = {
      JdbcUtil.mysqlJdbcDF("tbl_industry_etl_logic")(sqlContext)
    }

  }

}
