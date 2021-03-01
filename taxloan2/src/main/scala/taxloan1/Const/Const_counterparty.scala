package taxloan1.Const

import util.sparkTool

object Const_counterparty {

  val insert_latest_month_tmp: String =sparkTool.loader.loadFileString("/sql/taxloan1/Const_counterparty/insert_latest_month_tmp.sql")

  val insert_counterparty: String =sparkTool.loader.loadFileString("/sql/taxloan1/Const_counterparty/insert_counterparty.sql")

}
