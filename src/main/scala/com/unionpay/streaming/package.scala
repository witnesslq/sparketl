package com.unionpay

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by ywp on 2016/9/9.
  */
package object streaming extends Serializable {

  object SQLHiveContextSingleton extends Serializable {

    @transient private var instance: HiveContext = _

    def getInstance(@transient sparkContext: SparkContext): HiveContext = {
      synchronized {
        if (instance == null || sparkContext.isStopped) {
          instance = new HiveContext(sparkContext)
          //todo !!!
          instance.setConf("spark.sql.shuffle.partitions", "6")
        }
        instance
      }
    }
  }


}
