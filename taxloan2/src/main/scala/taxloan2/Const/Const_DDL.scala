package taxloan2.Const

import util.sparkTool

object Const_DDL {

  val create_dim_db: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_dim_db.sql")
  val create_db1: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/0000_create_db1.sql")
  val create_db2: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/0000_create_db2.sql")

  val create_cjlog: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/cjlog.sql")
  val create_saleinvoice: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/saleinvoice.sql")
  val create_saleinvoicedetail: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/saleinvoicedetail.sql")

  val create_bak_cjlog_tmp: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_bak_cjlog_tmp.sql")
  val create_bak_cross_month_tmp: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_bak_cross_month_tmp.sql")
  val create_bak_latest_month_tmp: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_bak_latest_month_tmp.sql")
  val create_bak_saleinvoice_tmp: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_bak_saleinvoice_tmp.sql")

  val create_c_p2: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_c_p2.sql")
  val create_cjlog_tmp: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_cjlog_tmp.sql")
  val create_control_table: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_control_table.sql")

  val create_counterparty: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_counterparty.sql")
  val create_counterparty_classify: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_counterparty_classify.sql")
  val create_counterparty_classify_incre: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_counterparty_classify_incre.sql")
  val create_counterparty_incre: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_counterparty_incre.sql")

  val create_cross_month_tmp: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_cross_month_tmp.sql")

  val create_dim_month: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_dim_month.sql")

  val create_latest_month_tmp: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_latest_month_tmp.sql")

  val create_mcht_tax: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_mcht_tax.sql")
  val create_mcht_tax_incre: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_mcht_tax_incre.sql")

  val create_p2: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_p2.sql")

  val create_saleinvoice_tmp: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_saleinvoice_tmp.sql")
  val create_saleinvoice_tmp1: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_saleinvoice_tmp1.sql")
  val create_saleinvoice_tmp2: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_saleinvoice_tmp2.sql")
  val create_saleinvoice_tmp3: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_saleinvoice_tmp3.sql")

  val create_statistics_crossmonth: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_statistics_crossmonth.sql")
  val create_statistics_crossmonth_incre: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_statistics_crossmonth_incre.sql")
  val create_statistics_month: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_statistics_month.sql")
  val create_statistics_month_incre: String =sparkTool.loader.getString("/sql/taxloan2/Const_DDL_Hive/create_statistics_month_incre.sql")

}
