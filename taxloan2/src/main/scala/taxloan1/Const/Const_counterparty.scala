package taxloan1.Const

import com.plj.scala.tools.loadString

object Const_counterparty {

  val insert_latest_month_tmp: String =loadString.getString("/sql/taxloan1/Const_counterparty/insert_latest_month_tmp.sql",this)

  val insert_counterparty: String =loadString.getString("/sql/taxloan1/Const_counterparty/insert_counterparty.sql",this)

}
