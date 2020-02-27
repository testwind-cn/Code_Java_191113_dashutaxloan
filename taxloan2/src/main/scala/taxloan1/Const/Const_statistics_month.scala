package taxloan1.Const

import com.plj.tools.scala.loadString

object Const_statistics_month {

  val insert_p2: String =loadString.getString("/sql/taxloan1/Const_statistics_month/insert_p2.sql",this)
  val insert_statistics_month: String =loadString.getString("/sql/taxloan1/Const_statistics_month/insert_statistics_month.sql",this)

}
