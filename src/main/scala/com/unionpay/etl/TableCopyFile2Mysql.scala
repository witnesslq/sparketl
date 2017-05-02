package com.unionpay.etl

import java.io.PrintWriter
import java.math.{BigDecimal => javaDecimal}
import java.nio.file.{Files, Path, Paths}
import java.sql.Timestamp
import java.util.stream.Collectors

import com.unionpay.db.jdbc.MysqlConnection
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Created by ywp on 2016/7/21.
  */
object TableCopyFile2Mysql {

  private val normalSables = Seq(
    "TBL_CHMGM_BRAND_INF",
    "TBL_CHMGM_BRAND_PIC_INF",
    "TBL_CHMGM_BUSS_DIST_INF",
    "TBL_CHMGM_MCHNT_PARA",
    "TBL_CHMGM_BRAND_COMMENT_INF",
    "TBL_CHMGM_BUSS_ADMIN_DIVISION_CD"
  )
  private val moreThan22Tables = Seq("TBL_CHMGM_CHARA_GRP_DEF_FLAT", "TBL_CHMGM_TICKET_COUPON_INF", "TBL_CHMGM_PREFERENTIAL_MCHNT_INF")
  private val root = "D:\\银联\\mongo数据\\0722导出表"
  //  private val root = "C:\\Users\\ywp\\Desktop"

  private lazy val delFiles = Files.walk(Paths.get(root))
    .collect(Collectors.toList[Path])
    .map(_.getFileName.toString)
    .filter(_.endsWith("del"))
    .toArray

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("TableCopyFile2Mysql--db2导出的del文件拷贝数据到mysql")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array())
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)
    val mysql = MysqlConnection.build("file2mysql", "sink")
    import mysql._
    delFiles.foreach(del => {
      val table = del.split("\\.").head.toLowerCase()
      val file = s"${root}\\${del}"
      val pw = new PrintWriter("C:\\Users\\ywp\\Desktop\\test.sql")
      if (table.toUpperCase == "TBL_CHMGM_BRAND_INF") {
        normalSables.map(_.toLowerCase).contains(table) match {
          case true => save(table, file)
          case _ =>
        }
      }

      pw.close()
    })

    sc.stop()

  }

  def save(table: String, file: String) = {
    import Scop._
    table.toUpperCase match {
      case "TBL_CHMGM_BRAND_INF" => {
//        val brands = CSVReader[Brand].readCSVFromFileName(file, true)
//        brands.foreach(println)
      }
      case "TBL_CHMGM_BRAND_PIC_INF" =>
      case "TBL_CHMGM_BUSS_DIST_INF" =>
      case "TBL_CHMGM_MCHNT_PARA" =>
      case "TBL_CHMGM_BRAND_COMMENT_INF" =>
      case "TBL_CHMGM_BUSS_ADMIN_DIVISION_CD" =>
    }
  }

}

object Scop {

  //超过了case class tuple超过22个字段的限制

  case class BrandComment(seq_id: Long, cdhd_usr_id: String, brand_id: Long, brand_nm: String, brand_comment_txt: String, brand_comment_st: String, rec_crt_ts: Timestamp, rec_upd_ts: Timestamp, ver_no: Int, feedback_txt: String, feedback_usr_id: String, feedback_ts: Timestamp)

  case class Brand(brand_id: Long, brand_nm: String, buss_bmp: String, cup_branch_ins_id_cd: String, avg_consume: java.math.BigDecimal, brand_desc: String, avg_comment: Long, brand_st: String, content_id: Int, rec_crt_ts: Timestamp, rec_upd_ts: Timestamp, brand_env_grade: Long, brand_srv_grade: Long, brand_popular_grade: Long, brand_taste_grade: Long, brand_tp: String, entry_ins_id_cd: String, entry_ins_cn_nm: String, rec_crt_usr_id: String, rec_upd_usr_id: String)

  case class BrandPic(seq_id: Long, brand_id: Long, brand_pic_nm: String, brand_pic_tp: String, client_tp: String, brand_pic_prio: Int, brand_pic_url: String, brand_pic_folder: String, rec_crt_ts: Timestamp, rec_upd_ts: Timestamp)

  case class AdminCd(admin_division_cd: String, admin_division_cn_nm: String, admin_division_en_nm: String, admin_division_cd_en: String, admin_division_lvl: String, prov_division_cd: String, city_division_cd: String, municipality_in: String, city_in: String, county_in: String, admin_region_cd: String, valid_in: String, cup_branch_ins_id_cd: String, city_prio: String)

  case class Dist(buss_dist_cd: Long, buss_dist_nm: String, buss_dist_order: Int, prov_division_cd: String, city_division_cd: String, dist_division_cd: String, rec_crt_ts: Timestamp, rec_upd_ts: Timestamp, ver_no: Int, entry_ins_id_cd: String, entry_ins_cn_nm: String)

  //case class Flat(rec_id: Long, chara_grp_cd: String, chara_grp_nm: String, chara_data: String, chara_data_tp: String, aud_usr_id: String, aud_idea: String, aud_ts: Timestamp, aud_in: String, oper_action: String, grp_rec_crt_usr_id: String, grp_rec_upd_usr_id: String, ver_no: Int, event_id: Int,
  //                oper_in: String, sync_st: String, grp_cup_branch_ins_id_cd: String, input_data_tp: String, grp_entry_ins_id_cd: String, grp_entry_ins_cn_nm: String,
  //                grp_entry_cup_branch_ins_id_cd: String, order_in_grp: Int, mchnt_cd: String, mchnt_nm: String, mchnt_addr: String, mchnt_phone: String, mchnt_url: String, mchnt_city_cd: String, mchnt_county_cd: String, mchnt_prov: String, buss_dist_cd: Long, mchnt_type_id: Long, cooking_style_id: Long, rebate_rate: Long, discount_rate: Long,
  //                mchnt_avg_consume: javaDecimal, point_mchnt_in: String, discount_mchnt_in: String, preferential_mchnt_in: String, opt_sort_seq: Int, keywords: String, mchnt_desc: String, encr_loc_inf: String, comment_num: Int, favor_num: Int, share_num: Int, park_inf: String, buss_hour: String, traffic_inf: String, famous_service: String,
  //                comment_value: Int, mchnt_content_id: Int, mchnt_st: String, mchnt_first_para: Long, mchnt_second_para: Long, mchnt_longitude: javaDecimal, mchnt_latitude: javaDecimal, mchnt_longitude_web: javaDecimal, mchnt_latitude_web: javaDecimal, mchnt_cup_branch_ins_id_cd: String, branch_nm: String, mchnt_buss_bmp: String, term_diff_store_tp_in: String, brand_id: Long,
  //                brand_nm: String, brand_buss_bmp: String, brand_cup_branch_ins_id_cd: String, brand_avg_consume: javaDecimal, brand_desc: String, avg_comment: Long, brand_st: String, brand_content_id: Int, brand_env_grade: Long, brand_srv_grade: Long, brand_popular_grade: Long, brand_taste_grade: Long, brand_tp: String, brand_entry_ins_id_cd: String, brand_entry_ins_cn_nm: String,
  //                brand_rec_crt_usr_id: String, brand_rec_upd_usr_id: String, rec_crt_ts: Timestamp, rec_upd_ts: Timestamp, amap_longitude: javaDecimal, amap_latitude: javaDecimal)

  case class Para(mchnt_para_id: Long, mchnt_para_cn_nm: String, mchnt_para_en_nm: String, mchnt_para_tp: String, mchnt_para_level: Int, mchnt_para_parent_id: Long, mchnt_para_order: Int, rec_crt_ts: Timestamp, rec_upd_ts: Timestamp, ver_no: Int)

  //case class Mchnt(mchnt_cd: String, mchnt_nm: String, mchnt_addr: String, mchnt_phone: String, mchnt_url: String, mchnt_city_cd: String, mchnt_county_cd: String, mchnt_prov: String, buss_dist_cd: Long, mchnt_type_id: Long, cooking_style_id: Long, rebate_rate: Long, discount_rate: Long, avg_consume: javaDecimal, point_mchnt_in: String, discount_mchnt_in: String,
  //                 preferential_mchnt_in: String, opt_sort_seq: Int, keywords: String, mchnt_desc: String, rec_crt_ts: Timestamp, rec_upd_ts: Timestamp, encr_loc_inf: String, comment_num: Int, favor_num: Int, share_num: Int, park_inf: String, buss_hour: String, traffic_inf: String, famous_service: String, comment_value: Int, content_id: Int, mchnt_st: String,
  //                 mchnt_first_para: Long, mchnt_second_para: Long, mchnt_longitude: javaDecimal, mchnt_latitude: javaDecimal, mchnt_longitude_web: javaDecimal, mchnt_latitude_web: javaDecimal, cup_branch_ins_id_cd: String, branch_nm: String, brand_id: Long, buss_bmp: String, term_diff_store_tp_in: String, rec_id: Long, amap_longitude: javaDecimal, amap_latitude: javaDecimal)

  //case class Coupon(bill_id: String, bill_nm: String, bill_desc: String, bill_short_desc: String, bill_tp: String, mchnt_cd: String, mchnt_nm: String, valid_begin_dt: String, valid_end_dt: String, disc_rate: Int, money_amt: javaDecimal
  //                  , low_trans_at_limit: Long, bill_st: String, oper_action: String, aud_st: String, aud_usr_id: String, aud_ts: Timestamp, aud_idea: String, rec_upd_usr_id: String, rec_upd_ts: Timestamp, rec_crt_usr_id: String, rec_crt_ts: Timestamp,
  //                  content_id: Int, valid_begin_tm: String, valid_end_tm: String, hot_ticket_coupon_in: String, chara_grp_cd: String, chara_grp_nm: String, obtain_chnl: String, cfg_tp: String, show_bill_in: String, preferential_activity_id: Long, mchnt_rebate_rate: Long, delay_use_days: Int, bill_rule_bmp: String, seckill_st: String, bill_restrict_desc: String,
  //                  download_in: String, cup_branch_ins_id_cd: String, carriage_in: String, plan_id: Int, mchnt_cd_list: String, iss_hdqrs_ins_id_cd_list: String, dwn_total_num: Long, dwn_num: Long, dwn_begin_dt: String, dwn_end_dt: String, seckill_begin_dt: String, plan_nm: String, dwn_num_limit: Long, preferential_cond: String, initial_dwn_num: Long, bill_reserve_bmp: String,
  //                  card_lvl_list: String, iss_ins_internal_cd_list: String, client_chnl: String, seckill_begin_tm: String, first_maktg_event_cd: String, second_maktg_event_cd: String, exclusive_tip: String, user_notice: String, package_info: String, exclusive_in: String, deploy_ts: Timestamp, sale_in: String, bill_original_price: Long, bill_price: Long, bill_sub_tp: String, price_trend_st: String,
  //                  pay_timeout: Int, auto_refund_st: String, anytime_refund_st: String, must_booking_st: String, imprest_tp_st: String, bill_remain_num: Long, token_rule_list: String, rand_tp: String, expt_val: javaDecimal, std_deviation: javaDecimal, rand_at_min: javaDecimal, rand_at_max: javaDecimal, disc_max_in: String, disc_max: Int, rand_period_tp: String, rand_period: String, card_attr_rule_list: String)
}

