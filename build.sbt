name := "sparketl"

version := "1.O"

scalaVersion := "2.10.6"

crossScalaVersions := Seq("2.10.0", "2.10.2", "2.10.3", "2.10.4", "2.10.5", "2.10.6")

assemblyJarName in assembly := "etl-assembly.jar"

test in assembly := {}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)


unmanagedJars in Compile += file("C:\\Users\\ywp\\Desktop\\jar\\magellan-1.0.4-SNAPSHOT.jar")
unmanagedJars in Compile += file("C:\\Users\\ywp\\Desktop\\jar\\spark-redis-0.5.1.jar")
unmanagedJars in Compile += file("C:\\Users\\ywp\\Desktop\\jar\\zookeeper-3.4.5-cdh5.8.0.jar")
unmanagedJars in Compile += file("C:\\Users\\ywp\\Desktop\\jar\\zkclient-0.7.jar")
unmanagedJars in Compile += file("C:\\Users\\ywp\\Desktop\\jar\\kafka_2.10-0.9.0-kafka-2.0.0.jar")
unmanagedJars in Compile += file("C:\\Users\\ywp\\Desktop\\jar\\kafka-clients-0.9.0-kafka-2.0.0.jar")
unmanagedJars in Compile += file("C:\\Users\\ywp\\Desktop\\jar\\snappy-java-1.1.2.6.jar")
unmanagedJars in Compile += file("C:\\Users\\ywp\\Desktop\\jar\\metrics-core-2.2.0.jar")

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  //  cp filter { x => !Seq("kafka-clients-0.8.2.1.jar", "kafka_2.10-0.8.2.1.jar").contains(x.data.getName) }
  cp
}

filterScalaLibrary := false

//dependencyDotFile := file("dependencies.dot")

libraryDependencies ++= {
  val spark_version = "1.6.2"
  Seq(
    "org.apache.spark" %% "spark-sql" % spark_version excludeAll (
      ExclusionRule(organization = "org.xerial.snappy")
      ),
    "org.apache.spark" %% "spark-mllib" % spark_version excludeAll (
      ExclusionRule(organization = "org.xerial.snappy")
      ),
    "org.mongodb.spark" %% "mongo-spark-connector" % "1.0.0",
    "mysql" % "mysql-connector-java" % "5.1.39",
    //  "org.apache.kafka" % "kafka-clients" % "0.10.0.0",
    //  "org.apache.kafka" % "kafka_2.10" % "0.10.0.0",
    /*"org.apache.kafka" % "kafka-clients" % "0.8.2.1",
    "org.apache.kafka" %% "kafka" % "0.8.2.1",*/
    "org.apache.spark" %% "spark-hive" % spark_version excludeAll (
      ExclusionRule(organization = "org.xerial.snappy")
      ),
    "org.apache.spark" %% "spark-streaming-kafka" % spark_version excludeAll(
      ExclusionRule(organization = "org.apache.zookeeper"),
      ExclusionRule(organization = "org.apache.kafka"),
      ExclusionRule(organization = "org.xerial.snappy")
      ),
    /* "org.apache.spark" %% "spark-streaming-kafka-0-8" % spark_version excludeAll(
       ExclusionRule(organization = "org.apache.zookeeper"),
       ExclusionRule(organization = "org.apache.kafka")
       ),*/
    //  使用自定义的停用词jar
    "com.hankcs" % "hanlp" % "portable-1.2.10",
    //  "com.iheart" %% "ficus" % "1.0.2"
    "net.ceedubs" %% "ficus" % "1.0.1",
    //  "harsha2010" % "magellan" % "1.0.3-s_2.10",
    //todo fixed 不支持sparK1.6 1.0.3-s_2.10 自己打包 支持多边形范围的spark dsl
    //  "harsha2010" % "magellan" % "1.0.4-SNAPSHOT",
    "graphframes" % "graphframes" % "0.1.0-spark1.6",
    "org.elasticsearch" %% "elasticsearch-spark" % "2.3.3",
    //引入spark-redis
    //  "RedisLabs" % "spark-redis" % "0.3.0",
    "redis.clients" % "jedis" % "2.8.0",
    //也支持地理位置计算
    "ch.hsr" % "geohash" % "1.3.0",
    //scala excel
    "org.apache.poi" % "poi" % "3.14",
    "org.apache.poi" % "poi-ooxml" % "3.14",
    "org.scalatest" %% "scalatest" % "2.2.6" % "test"
    //    "com.sksamuel.elastic4s" % "elastic4s-core_2.10" % "2.3.1"
  )
}
