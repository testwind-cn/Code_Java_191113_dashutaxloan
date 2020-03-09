package taxloan2.Const

import util.sparkTool

object Const_statistics_crossmonth {

  val insert_c_p2: String =sparkTool.loader.getString("/sql/taxloan2/Const_statistics_crossmonth/insert_c_p2.sql")

  val insert_statistics_crossmonth: String =sparkTool.loader.getString("/sql/taxloan2/Const_statistics_crossmonth/insert_statistics_crossmonth.sql")

}
