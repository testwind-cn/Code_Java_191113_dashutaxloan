package dashu.constant_common

import com.plj.tools.scala.loadString

object Const_common {
  val global_set_01: Array[String] =loadString.getHiveStringList("/sql/dashu/Const_Common/global_set_01.sql",this)
  val create_dim_db: String =loadString.getString("/sql/dashu/Const_DDL_Hive/create_dim_db.sql",this)
  val create_dim_date: String =loadString.getString("/sql/dashu/Const_DDL_Hive/create_dim_date.sql",this)
  val idno_area_mapping: String =loadString.getString("/sql/dashu/Const_DDL_Hive/idno_area_mapping.sql",this)
  val idno_province_mapping: String =loadString.getString("/sql/dashu/Const_DDL_Hive/idno_province_mapping.sql",this)
}
