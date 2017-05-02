package com.unionpay.etl

import com.unionpay.db.jdbc.JdbcUtil
import com.unionpay.util.{ConfigUtil, IdGenerator}
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.unionpay.db.mongo.MongoUtil._

/**
  * Created by ywp on 2016/7/7.
  */
object BrandInfoJob {

  def main(args: Array[String]) {

    if (args.size != 3) {
      System.err.println("useAge: crawl true or false unionpay true or false jobDateTime")
      System.exit(1)
    }

    val conf = new SparkConf()
      .setAppName("BrandInfoJob--品牌信息任务")
      //      .setMaster("local[4]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.yarn.driver.memoryOverhead", "1024")
      .set("spark.yarn.executor.memoryOverhead", "2000")
      .set("spark.network.timeout", "300s")
      .set("spark.executor.heartbeatInterval", "30s")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")
    etl(args)
    sc.stop()
  }

  def etl(args: Array[String])(implicit sqlContext: SQLContext) = {
    val brandInfoDF = fetchUnionBrand
    if (args(1) == "true") etlUnion(brandInfoDF)
    val crawlDF = fetchCrawlBrand(brandInfoDF)
    if (args(0) == "true") etlCrawl(crawlDF)
  }

  def fetchLogicDf(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val subTb = "tbl_content_industry_sub_inf"
    val subDF = JdbcUtil.mysqlJdbcDF(subTb, "sink").selectExpr("industry_no", "industry_sub_no", "industry_sub_nm")
    val industryLogicDF = JdbcUtil.mysqlJdbcDF("tbl_industry_etl_logic", "sink").selectExpr("type", "type_name", "origin_name", "real_name")
    industryLogicDF.as('a)
      .join(subDF.as('b), trim($"a.real_name") === trim($"b.industry_sub_nm"))
      .selectExpr("trim(a.origin_name)  origin_name", "trim(b.industry_sub_nm)  industry_sub_nm",
        "trim(b.industry_no) industry_no", "trim(b.industry_sub_no)  industry_sub_no")
  }

  def fetchCrawlBrand(brandInfoDF: DataFrame)(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._

    //todo  mongo存在数据一致性问题 见 MongoUtil
    val df = sqlContext.mongoDF("brand")
      .select(trim($"name").as("name"), trim($"category").as("category"), trim($"subCategory").as("subCategory"))
      .groupBy($"name").agg(first($"category").as("category"), first($"subCategory").as("subCategory"))

    val categoryDF = JdbcUtil.mysqlJdbcDF("tbl_content_industry_inf", "sink").selectExpr("industry_no", "trim(industry_nm) industry_nm")
    val logicDF = fetchLogicDf

    val unionBrandNames = brandInfoDF.selectExpr("trim(BRAND_NM) BRAND_NM").distinct()
      .map(_.getAs[String]("BRAND_NM")).collect()
    //互联网品牌中去重银联自有品牌 no anti join
    val crawlDF = df.filter(!$"name".isin(unionBrandNames: _*))
      .selectExpr("getBrandNo()  brand_no", "name brand_nm", "category", "subCategory")

    //先从二级分类匹配 确定属于的一二级分类 如果找不到 再匹配一级分类 如果有对应到一级分类则确定一级分类
    //没有二级分类的
    val noSubCatDF = crawlDF.filter("subCategory=''")
    //有二级分类的
    val subCatDF = crawlDF.filter("subCategory!=''")

    val hasSubDF = subCatDF.as('a)
      .join(broadcast(logicDF).as('c), $"a.subCategory" === $"c.origin_name", "left_outer")
      .selectExpr("a.brand_no", "a.brand_nm", "coalesce(c.industry_no,'') industry_no", "coalesce(c.industry_sub_no,'') industry_sub_no")

    //杭州数据有使用宋恺整理的二级分类当一级分类的情况 使用一级分类去匹配整理的二级分类表
    val useParAsSubDF = noSubCatDF.as('a)
      .join(logicDF.as('b), $"a.category" === $"b.origin_name")
      .selectExpr("a.brand_no", "a.brand_nm", "b.industry_no", "b.industry_sub_no")

    val excludeNos = useParAsSubDF.selectExpr("brand_no").map(_.getAs[String]("brand_no")).collect()

    //没有二级分类 并且不能使用一级分类匹配二级分类的
    val onlyHasParDF = noSubCatDF.filter(!$"brand_no".isin(excludeNos: _*)).as('a)
      .join(categoryDF.as('b), $"a.category" === $"b.industry_nm", "left_outer")
      .selectExpr("a.brand_no", "a.brand_nm", "coalesce(b.industry_no,'') industry_no", "'' industry_sub_no")


    //三种情况合并
    hasSubDF.unionAll(useParAsSubDF).unionAll(onlyHasParDF).addAlwaysColumn

  }


  def fetchUnionBrand(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    //生成brandNo
    sqlContext.udf.register("getBrandNo", () => IdGenerator.generateBrandID)
    val brandInfoTb = "TBL_CHMGM_BRAND_INF"
    val brandInfoDF = JdbcUtil.mysqlJdbcDF(brandInfoTb)
    brandInfoDF
  }

  def etlCrawl(crawlDF: DataFrame)(implicit sqlContext: SQLContext) = {
    JdbcUtil.save2Mysql("tbl_content_brand_inf")(crawlDF)
  }


  def etlUnion(brandInfoDF: DataFrame)(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    val brandImgTb = "TBL_CHMGM_BRAND_PIC_INF"
    val config = readConfig.union
    val tmpBrandInfoDF = brandInfoDF
      .reNameColumn(config.transForm)
      .appendDefaultVColumn(config.default)
      .addAlwaysColumn
    val brandImgDF = JdbcUtil.mysqlJdbcDF(brandImgTb)
      .filter($"CLIENT_TP" === lit("2"))
      .withColumnRenamed("BRAND_ID", "BRAND_NO")
    save2Mysql(tmpBrandInfoDF, brandImgDF)
  }

  def fetchBrandImg(brandInfoDF: DataFrame, brandImgDF: DataFrame)(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val imgDF = brandImgDF.as('a).join(brandInfoDF.as('b), $"a.BRAND_NO" === $"b.BRAND_NO", "leftsemi")
      .selectExpr("concat('https://youhui.95516.com/app/image/brand/',SEQ_ID,'.jpg') as BRAND_IMG", "a.BRAND_NO")
      .groupBy($"BRAND_NO").agg(first($"BRAND_IMG").as("BRAND_IMG"))
    imgDF
  }

  def readConfig: BrandInfoConfig = {
    import net.ceedubs.ficus.Ficus._
    val configName = "brandInfo"
    ConfigUtil.readClassPathConfig[BrandInfoConfig](configName)
  }

  def save2Mysql(brandInfoDF: DataFrame, brandImgDF: DataFrame)(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    val brandInfTb = "tbl_content_brand_inf"
    val imgDF = fetchBrandImg(brandInfoDF, brandImgDF)
    val df = brandInfoDF.as('a)
      .join(imgDF.as('b), $"a.BRAND_NO" === $"b.BRAND_NO", "left_outer")
      .select($"a.BRAND_NO".cast(StringType).as("BRAND_NO"), trim($"a.BRAND_NM").as("BRAND_NM"), trim($"a.BRAND_INTRO").as("BRAND_INTRO"),
        when(length(trim($"a.BRAND_DESC")) > 1024, substring(trim($"a.BRAND_DESC"), 0, 1024))
          .otherwise(trim($"a.BRAND_DESC")).as("BRAND_DESC"), $"a.ROW_CRT_USR", $"a.ROW_CRT_TS",
        $"a.REC_UPD_USR", $"a.REC_UPD_TS", coalesce($"b.BRAND_IMG", lit("")).as("BRAND_IMG"))
      .filter("trim(BRAND_NM)!=''")

    JdbcUtil.save2Mysql(brandInfTb)(df)

  }

}


case class BrandInfo(transForm: Map[String, String], default: Map[String, String])

case class BrandInfoConfig(union: BrandInfo)