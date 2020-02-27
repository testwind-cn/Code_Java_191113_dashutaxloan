package taxloan1.Const

import com.plj.tools.scala.loadString

object Const_table_export {

  val insert_mcht_tax_incre: String =loadString.getString("/sql/taxloan1/Const_table_export/insert_mcht_tax_incre.sql",this)
  val insert_counterparty_incre: String =loadString.getString("/sql/taxloan1/Const_table_export/insert_counterparty_incre.sql",this)
  val insert_counterparty_classify_incre: String =loadString.getString("/sql/taxloan1/Const_table_export/insert_counterparty_classify_incre.sql",this)
  val insert_statistics_month_incre: String =loadString.getString("/sql/taxloan1/Const_table_export/insert_statistics_month_incre.sql",this)
  val insert_statistics_crossmonth_incre: String =loadString.getString("/sql/taxloan1/Const_table_export/insert_statistics_crossmonth_incre.sql",this)

}