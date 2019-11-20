package dashu2.constant_common

import com.plj.scala.tools.loadString

object Const_common {
  val DATABASE = "dm_taxloan"
  val global_set_01: Array[String] =loadString.getHiveStringList("/sql/dashu/Const_Common/global_set_01.sql",this)
}
