package taxloan1.Const

import com.plj.tools.scala.loadString

object Const_counterparty_classify {

  val insert_counterparty_classify: String =loadString.getString("/sql/taxloan1/Const_counterparty_classify/insert_counterparty_classify.sql",this)

}
