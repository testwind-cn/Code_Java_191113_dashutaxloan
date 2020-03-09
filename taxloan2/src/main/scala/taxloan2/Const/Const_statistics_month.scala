package taxloan2.Const

import util.sparkTool

object Const_statistics_month {

  val insert_p2: String =sparkTool.loader.getString("/sql/taxloan2/Const_statistics_month/insert_p2.sql")
  val insert_statistics_month: String =sparkTool.loader.getString("/sql/taxloan2/Const_statistics_month/insert_statistics_month.sql")

}
