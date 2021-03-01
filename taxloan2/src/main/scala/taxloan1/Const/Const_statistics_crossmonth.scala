package taxloan1.Const

import util.sparkTool

object Const_statistics_crossmonth {

  val insert_c_p2: String =sparkTool.loader.loadFileString("/sql/taxloan1/Const_statistics_crossmonth/insert_c_p2.sql")

  val insert_statistics_crossmonth: String =sparkTool.loader.loadFileString("/sql/taxloan1/Const_statistics_crossmonth/insert_statistics_crossmonth.sql")

}
