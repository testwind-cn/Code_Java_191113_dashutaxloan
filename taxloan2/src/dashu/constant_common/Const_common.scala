package dashu.constant_common

import com.plj.scala.tools.loadString

object Const_common {
  val global_set_01: Array[String] =loadString.getHiveStringList("/sql/dashu/Const_Common/global_set_01.sql",this)
  val create_dim_date: String =loadString.getString("/sql/dashu/Const_DDL_Hive/create_dim_date.sql",this)
}
