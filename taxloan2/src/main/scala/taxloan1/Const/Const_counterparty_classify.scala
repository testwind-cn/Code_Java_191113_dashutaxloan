package taxloan1.Const

import util.sparkTool

object Const_counterparty_classify {

  val insert_counterparty_classify: String =sparkTool.loader.getString("/sql/taxloan1/Const_counterparty_classify/insert_counterparty_classify.sql")

}
