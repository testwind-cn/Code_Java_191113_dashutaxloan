package taxloan2.Const

import util.sparkTool

object Const_mcht_full_incre {


  val insert_cjlog_tmp_full: String =sparkTool.loader.loadFileString("/sql/taxloan2/Const_mcht_full_incre/insert_cjlog_tmp_full.sql")
  val insert_cjlog_tmp_incre: String =sparkTool.loader.loadFileString("/sql/taxloan2/Const_mcht_full_incre/insert_cjlog_tmp_incre.sql")
  val insert_cjlog_tmp_zpp: String =sparkTool.loader.loadFileString("/sql/taxloan2/Const_mcht_full_incre/insert_cjlog_tmp_zpp.sql")

  val insert_saleinvoice_tmp1_full: String =sparkTool.loader.loadFileString("/sql/taxloan2/Const_mcht_full_incre/insert_saleinvoice_tmp1_full.sql")
  val insert_saleinvoice_tmp1_incre: String =sparkTool.loader.loadFileString("/sql/taxloan2/Const_mcht_full_incre/insert_saleinvoice_tmp1_incre.sql")


  val insert_saleinvoice_tmp2_full: String =sparkTool.loader.loadFileString("/sql/taxloan2/Const_mcht_full_incre/insert_saleinvoice_tmp2_full.sql")
  val insert_saleinvoice_tmp2_incre: String =sparkTool.loader.loadFileString("/sql/taxloan2/Const_mcht_full_incre/insert_saleinvoice_tmp2_incre.sql")


  val insert_saleinvoice_tmp: String =sparkTool.loader.loadFileString("/sql/taxloan2/Const_mcht_full_incre/insert_saleinvoice_tmp.sql")


  val insert_mcht_tax: String =sparkTool.loader.loadFileString("/sql/taxloan2/Const_mcht_full_incre/insert_mcht_tax.sql")

  val insert_cross_month_tmp_full: String =sparkTool.loader.loadFileString("/sql/taxloan2/Const_mcht_full_incre/insert_cross_month_tmp_full.sql")
  val insert_cross_month_tmp_incre: String =sparkTool.loader.loadFileString("/sql/taxloan2/Const_mcht_full_incre/insert_cross_month_tmp_incre.sql")


}
