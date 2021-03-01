package taxloan1.Const

import util.sparkTool

object Const_table_export {

  val insert_mcht_tax_incre: String =sparkTool.loader.loadFileString("/sql/taxloan1/Const_table_export/insert_mcht_tax_incre.sql")
  val insert_counterparty_incre: String =sparkTool.loader.loadFileString("/sql/taxloan1/Const_table_export/insert_counterparty_incre.sql")
  val insert_counterparty_classify_incre: String =sparkTool.loader.loadFileString("/sql/taxloan1/Const_table_export/insert_counterparty_classify_incre.sql")
  val insert_statistics_month_incre: String =sparkTool.loader.loadFileString("/sql/taxloan1/Const_table_export/insert_statistics_month_incre.sql")
  val insert_statistics_crossmonth_incre: String =sparkTool.loader.loadFileString("/sql/taxloan1/Const_table_export/insert_statistics_crossmonth_incre.sql")

}