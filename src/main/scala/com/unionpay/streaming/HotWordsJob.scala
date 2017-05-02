package com.unionpay.streaming

import com.redislabs.provider.redis._
import com.unionpay.util.JsonUtil
import com.unionpay.util.RedisOps._
import kafka.message.MessageAndMetadata
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/**
  * todo spark.io.compression.codec=lzf snappy包冲突
  * todo  使用cdh默认的kafka jar包
  * todo https://issues.apache.org/jira/browse/SPARK-6152
  * Created by ywp on 2016/7/25.
  */
object HotWordsJob {

  //程序手动写的消费点
  private lazy val zkNode = "/hotWords/hotWordsConsumer"
  private lazy val shopBrandPath = "/hotword/shopbrand"
  //todo
  //  private lazy val shopBrandPath = "D:\\shop\\parquet"
  private lazy val historySearchKey = "historySearch"
  private lazy val hotBrandSetKey = "hotBrandSet"
  private lazy val hotLandmarkHashKey = "hotLandmarkHash"

  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: topic interval")
      System.exit(1)
    }

    //spark streaming 自己的checkPoint
    //    val checkPoint = "D:\\streamingPoint"
    val checkPoint = "/checkPoint/hotWords"
    val ssc = StreamingContext.getOrCreate(checkPoint, () => {
      createSSC(args, checkPoint, zkNode)
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def createSSC(args: Array[String], checkpointPath: String, kafkaSavePath: String): StreamingContext = {

    //用户历史搜索与当前窗口合并去重
    val genSearchList = udf((redisSeq: Seq[String], windowSeq: Seq[String]) => {
      val total = windowSeq.size
      val strList = total match {
        case num: Int if num >= 5 => {
          val hts = windowSeq.map(JsonUtil.readAsBeanByJson4s[HotWord])
          hts.sortBy(-_.msgTs.getOrElse(0L)).take(5).mkString("[", ",", "]")
        }
        case _: Int => {
          val buffer = windowSeq.toBuffer
          buffer.appendAll(redisSeq)
          val hts = buffer.take(5).map(JsonUtil.readAsBeanByJson4s[HotWord])
          hts.groupBy(_.input.trim).mapValues(_.head).values.toSeq.sortBy(-_.msgTs.getOrElse(0L)).map(_.searchJsonStringValue).mkString("[", ",", "]")
        }
      }
      strList.trim.replaceAll("\\\\r\\\\n", "")
    })

    val genJsonString = udf((brandId: String, brandName: String) => {

      s"""
         | {"brandId":"${brandId}","brandName":"${brandName}"}
       """.stripMargin.trim
    })

    val conf = globalConf(args).buildRedis
    val ssc = new StreamingContext(conf, Seconds(args(1).toInt))
    ssc.checkpoint(checkpointPath)

    val topicsSet = args(0).split(",").toSet
    //todo 如果不存在topic先创建
    //    KafkaTopicAutoCreator.createTopics(topicsSet)

    val confMap = conf.getAll.toMap

    //自己存储kafka offset,程序升级后可直接使用历史偏移,只需要把原有的checkpoint删除
    val fromOffset = TopicOffset(kafkaSavePath, confMap).getOffset(topicsSet)

    //    val fromOffset = CuratorTopicOffSet(kafkaSavePath, confMap).getOffset(topicsSet)
    val messageHandler = (mmd: MessageAndMetadata[Array[Byte], String]) => (mmd.key, mmd.message)
    val messages = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder, (Array[Byte], String)](
      ssc, conf.getAll.toMap, fromOffset, messageHandler)

    val illegalHotWord = HotWord("illegal", Option("illegal"), "illegal", "illegal", "illegal", "illegal", "illegal", None)

    val messageDStream = messages
      .transform(rdd => {

        //todo rdd 直接filter之后 不能转化为 HasOffsetRanges
        /* //校验数据的正确性
         val rdd = ordd.filter {
           case (_, str) => filterJsonFun(str)
         }*/


        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //      CuratorTopicOffSet(kafkaSavePath, confMap).saveOffset(offsetRanges)
        TopicOffset(kafkaSavePath, confMap).saveOffset(offsetRanges)


        val jsonDsRDD = rdd.mapPartitions(hit => hit.map {
          case (_, ws) => {
            try {
              if (ws.trim.startsWith("[")) JsonUtil.readAsBeanByJson4s[Seq[HotWord]](ws) else Seq(JsonUtil.readAsBeanByJson4s[HotWord](ws))
            } catch {
              case e: Exception => println(s"json format error : ${e.getMessage}"); Seq(illegalHotWord)
            }
          }
        }).mapPartitions(_.flatten)

        val sc = rdd.context
        sc.setLogLevel("ERROR")
        val initHost = sc.getConf.getInitHost
        //      val sqlContext = SQLContext.getOrCreate(sc)
        val sqlContext = SQLHiveContextSingleton.getInstance(sc)

        import sqlContext.implicits._


        val searchDF = jsonDsRDD
          .mapPartitions(it => it.map(x => {
            (x.userId, Seq(x))
          }))
          .reduceByKey((ht1, ht2) => {
            val sMap1 = ht1.map(x => (x.input.trim, x)).toMap
            val sMap2 = ht2.map(x => (x.input.trim, x)).toMap
            val x = sMap1.++(sMap2)
            x.values.toSeq
          })
          .mapPartitions(it => {
            it.map {
              case (uid, ws) => {
                (uid, ws.map(_.searchJsonStringValue).filter(!_.contains("illegal")))
              }
            }
          })
          .toDF("userId", "windowSearchList")
          .filter(size($"windowSearchList") > 0)


        val historySearchRedisRDD = sc.fromRedisKeyPattern(initHost, historySearchKey).getHash()

        val historySearchRedisDF = historySearchRedisRDD.toDF("userId", "redisSearchList")

        if (historySearchRedisDF.rdd.isEmpty) {

          if (!searchDF.rdd.isEmpty()) {
            val windowRDD = searchDF.mapPartitions(it => {
              it.map(row => {
                (row.getAs[String]("userId"), row.getAs[Seq[String]]("windowSearchList").take(5).mkString("[", ",", "]"))
              })
            })

            sc.toRedisHASH(windowRDD, historySearchKey, initHost)
          }

        } else {
          if (!searchDF.rdd.isEmpty()) {

            val riHistorySearchRedisDF = historySearchRedisDF.mapPartitions(it => {
              it.map(row => {
                val userId = row.getAs[String]("userId")
                val redisSearchList = row.getAs[String]("redisSearchList")
                val strList = JsonUtil.readAsBeanByJson4s[Seq[HotWord]](redisSearchList).map(_.searchJsonStringValue)
                (userId, strList)
              })
            })
              .toDF("userId", "redisSearchList")

            //历史用户数据统计
            val historySearchDF = riHistorySearchRedisDF.as('a)
              .join(searchDF.as('b), $"a.userId" === $"b.userId")
              .select($"a.userId", genSearchList($"a.redisSearchList", $"b.windowSearchList").as("searchList"))

            val historySearchRDD = historySearchDF.mapPartitions(it => {
              it.map(r => (r.getAs[String]("userId"), r.getAs[String]("searchList")))
            })

            val historyUserDF = historySearchDF.selectExpr("userId")
            val windowUserDF = searchDF.selectExpr("userId")

            //新增用户
            val newUserDF = windowUserDF.except(historyUserDF)

            //新用户统计
            val newSearchDF = searchDF.as('a)
              .join(newUserDF.as('b), $"a.userId" === $"b.userId", "leftsemi")
              .selectExpr("a.*")


            val newSearchRDD = newSearchDF.mapPartitions(it => {
              it.map(row => {
                val windowList = row.getAs[Seq[String]]("windowSearchList").take(5)
                (row.getAs[String]("userId"), windowList.mkString("[", ",", "]"))
              })
            })

            val allSearchRDD = historySearchRDD.union(newSearchRDD)

            sc.toRedisHASH(allSearchRDD, historySearchKey, initHost)

          }

        }


        val tmpRDD = jsonDsRDD.mapPartitions(hit => hit.map {
          ht => {
            ht match {
              case hw@HotWord(_, brandId, _, _, ty, _, _, _) if ty == "05" && brandId.isDefined && ty != "illegal" => (hw.logicKey, 0.33)
              case hw@HotWord(_, _, _, _, ty, _, _, _) if Seq("01", "04", "03").contains(ty) && ty != "illegal" => (hw.logicKey, 1.0)
              case _ => ("illegal", 0.0)
            }
          }
        }
        )

        tmpRDD.filter(x => (x._1 != "illegal") && (!x._1.contains("null")))

      })

    //todo reduceByKeyAndWindow 全局统计方式对redis读过频繁
    messageDStream
      .reduceByKeyAndWindow((x: Double, y: Double) => x + y, Seconds(10), Seconds(10))
      .foreachRDD(rdd => {
        val sc = rdd.context
        sc.setLogLevel("ERROR")
        val initHost = sc.getConf.getInitHost
        val sqlContext = SQLHiveContextSingleton.getInstance(sc)
        import sqlContext.implicits._
        val shopDF = sqlContext.read.parquet(shopBrandPath).cache()
        val tmpDF = rdd.toDF("idType", "totalCount").selectExpr("split(idType,':')  pairs", "totalCount")
          .selectExpr("pairs[0] id", "pairs[1] ty", "pairs[2] brandId", "totalCount").filter("totalCount>0")
        val windowLandMarkStreamDF = tmpDF.filter("ty='03'")
        val brandStreamDF = tmpDF.filter($"ty".isin(Seq("01", "04", "05"): _*))

        val historyLandMarkRedisRDD = sc.fromRedisKeyPattern(initHost, hotLandmarkHashKey).getHash()
        val historyLandMarkRedisDF = historyLandMarkRedisRDD.toDF("key", "totalCount")

        val hotBrandSetRedisRDD = sc.fromRedisKeyPattern(initHost, hotBrandSetKey).getZSet()
        val hotBrandSetRedisDF = hotBrandSetRedisRDD.toDF("key", "totalCount")

        if (historyLandMarkRedisDF.rdd.isEmpty()) {
          if (!windowLandMarkStreamDF.rdd.isEmpty()) {
            val redisLandRdd = windowLandMarkStreamDF.selectExpr("id key", "cast(totalCount as string) totalCount")
              .mapPartitions(
                it => {
                  it.map(r => {
                    (r.getAs[String]("key"), r.getAs[String]("totalCount"))
                  })
                }
              )
            sc.toRedisHASH(redisLandRdd, hotLandmarkHashKey, initHost)
          }

        } else {

          if (!windowLandMarkStreamDF.rdd.isEmpty()) {

            //出现过的用户叠加
            val historyLandRDD = historyLandMarkRedisDF.as('a)
              .join(windowLandMarkStreamDF.as('b), $"a.key" === $"b.id")
              .selectExpr("a.key", "cast((a.totalCount+cast(b.totalCount as double)) as string) totalCount")
              .mapPartitions(
                it => {
                  it.map(r => {
                    (r.getAs[String]("key"), r.getAs[String]("totalCount"))
                  })
                }
              )


            //新用户
            val newLandIdDF = windowLandMarkStreamDF.selectExpr("id key").except(historyLandMarkRedisDF.selectExpr("key"))
            val newLandRDD = windowLandMarkStreamDF.as('a)
              .join(newLandIdDF.as('b), $"a.id" === $"b.key")
              .selectExpr("id key", "cast(totalCount as string) totalCount")
              .mapPartitions(
                it => {
                  it.map(r => {
                    (r.getAs[String]("key"), r.getAs[String]("totalCount"))
                  })
                }
              )

            val allRedisLandRdd = historyLandRDD.union(newLandRDD)

            sc.toRedisHASH(allRedisLandRdd, hotLandmarkHashKey, initHost)

          }

        }

        if (hotBrandSetRedisDF.rdd.isEmpty()) {

          if (!brandStreamDF.rdd.isEmpty) {
            val redisBrandRdd = shopDF.as('a)
              .join(brandStreamDF.as('b), $"a.brandId" === $"b.brandId")
              .select(genJsonString($"a.brandId", $"a.brandName").as("key"), $"b.totalCount".cast(StringType).as("totalCount"))
              .mapPartitions(it => {
                it.map(r => {
                  (r.getAs[String]("key"), r.getAs[String]("totalCount"))
                })
              })

            sc.toRedisZSET(redisBrandRdd, hotBrandSetKey, initHost)
          }
        } else {

          if (!brandStreamDF.rdd.isEmpty) {

            val windowRedisBrandDF = shopDF.as('a)
              .join(brandStreamDF.as('b), $"a.brandId" === $"b.brandId")
              .select(genJsonString($"a.brandId", $"a.brandName").as("key"), $"b.totalCount")

            //历史用户
            val hotBrandHistoryRDD = hotBrandSetRedisDF.as('a)
              .join(windowRedisBrandDF.as('b), $"a.key" === $"b.key")
              .selectExpr("a.key", "cast((b.totalCount+cast(a.totalCount as double)) as string) totalCount")
              .mapPartitions(it => {
                it.map(r => {
                  (r.getAs[String]("key"), r.getAs[String]("totalCount"))
                })
              })

            //新用户
            val newHotBrandIdDF = windowRedisBrandDF.selectExpr("key").except(hotBrandSetRedisDF.selectExpr("key"))
            val newHotBrandRDD = windowRedisBrandDF.as('a)
              .join(newHotBrandIdDF.as('b), $"a.key" === $"b.key", "leftsemi")
              .selectExpr("a.key", "cast(a.totalCount as string) totalCount")
              .mapPartitions(it => {
                it.map(r => {
                  (r.getAs[String]("key"), r.getAs[String]("totalCount"))
                })
              })

            val allRedisBrandRdd = hotBrandHistoryRDD.union(newHotBrandRDD)

            sc.toRedisZSET(allRedisBrandRdd, hotBrandSetKey, initHost)

          }

        }

      })

    //todo mapWithState 全局统计方式对redis写过频繁
    /* messageDStream
       .mapWithState(StateSpec.function(messageMappingFunc _))
       .stateSnapshots()
       .foreachRDD(rdd => {
         val sc = rdd.context
         sc.setLogLevel("ERROR")
         val initHost = sc.getConf.getInitHost
         val sqlContext = SQLHiveContextSingleton.getInstance(sc)

         import sqlContext.implicits._
         val shopDF = sqlContext.read.parquet(shopBrandPath).cache()

         val tmpDF = rdd.toDF("idType", "totalCount").selectExpr("split(idType,':')  pairs", "totalCount")
           .selectExpr("pairs[0] id", "pairs[1] ty", "pairs[2] brandId", "totalCount").filter("totalCount>0")


         val landMarkStreamDF = tmpDF.filter("ty='03'")

         if (!landMarkStreamDF.rdd.isEmpty()) {
           val redisLandRdd = landMarkStreamDF.selectExpr("id key", "cast(totalCount as string) totalCount")
             .mapPartitions(
               it => {
                 it.map(r => {
                   (r.getAs[String]("key"), r.getAs[String]("totalCount"))
                 })
               }
             )
           sc.toRedisHASH(redisLandRdd, hotLandmarkHashKey, initHost)
         }

         val brandStreamDF = tmpDF.filter("ty='01' or ty='04'")

         if (!brandStreamDF.rdd.isEmpty) {
           val redisBrandRdd = shopDF.as('a)
             .join(brandStreamDF.as('b), $"a.brandId" === $"b.brandId")
             .select(genJsonString($"a.brandId", $"a.brandName").as("key"), $"b.totalCount".cast(StringType).as("totalCount"))
             .mapPartitions(it => {
               it.map(r => {
                 (r.getAs[String]("key"), r.getAs[String]("totalCount"))
               })
             })
           sc.toRedisZSET(redisBrandRdd, hotBrandSetKey, initHost)
         }
       })*/
    ssc
  }

  def messageMappingFunc(hotKey: String, one: Option[Double], state: State[Double]): Option[(String, Double)] = {
    val sum = one.getOrElse(0.0) + state.getOption.getOrElse(0.0)
    val output = Option(hotKey, sum)
    state.update(sum)
    output
  }

  def globalConf(args: Array[String]): SparkConf = {
    val zkList = KafkaZKConfig.getDefaultConf().zkList
    val brokerList = KafkaZKConfig.getDefaultConf().brokerList
    val conf = new SparkConf()
      .setAppName("HotWordsJob-热词分析")
      //todo
      //      .setMaster("local[*]")
      .set("spark.streaming.receiver.maxRate", "-1")
      .set("spark.streaming.blockInterval", "1s")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", "100000")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("group.id", "hotWordsJob")
      .set("zookeeper.connect", zkList)
      .set("client.id", "hotWords")
      .set("metadata.broker.list", brokerList)
      .set("serializer.class", "kafka.serializer.StringEncoder")
      .set("key.serializer.class", "kafka.serializer.StringEncoder")
      .set("spark.driver.userClassPathFirst", "true")
      .set("spark.executor.userClassPathFirst", "true")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.yarn.driver.memoryOverhead", "256")
      .set("spark.yarn.executor.memoryOverhead", "2000")
      .registerKryoClasses(Array(classOf[HotWord]))
    conf
  }
}

case class HotWord(id: String, brandId: Option[String], cityCd: String,
                   userId: String, `type`: String, value: String, input: String, msgTs: Option[Long]) {

  def logicKey: String = {
    `type` match {
      case "01" | "03" | "04" | "05" => s"$id:${`type`}:${brandId getOrElse "0"}"
    }
  }

  def searchJsonStringValue =
    s"""{"userId":"${userId}","type":"${`type`}","id":"${id}","brandId":"${brandId getOrElse "0"}","value":"${value}","cityCd":"${cityCd}","input":"${input}","msgTs":${msgTs.getOrElse(0L)}}""".stripMargin.trim

}
