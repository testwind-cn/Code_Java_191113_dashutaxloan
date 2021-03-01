package taxloan2.Const

import util.sparkTool

object Const_counterparty_classify {

  val insert_counterparty_classify: String =sparkTool.loader.loadFileString("/sql/taxloan2/Const_counterparty_classify/insert_counterparty_classify.sql")

}
