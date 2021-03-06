package dashu.constant_common

import util.sparkTool

object Const_common {
  val global_set_01: Array[String] =sparkTool.loader.getListFromFile("/sql/dashu/Const_Common/global_set_01.sql", "--", ";", 5 )
  val create_dim_db: String =sparkTool.loader.loadFileString("/sql/dashu/Const_DDL_Hive/create_dim_db.sql")
  val create_dim_date: String =sparkTool.loader.loadFileString("/sql/dashu/Const_DDL_Hive/create_dim_date.sql")
  val idno_area_mapping: String =sparkTool.loader.loadFileString("/sql/dashu/Const_DDL_Hive/idno_area_mapping.sql")
  val idno_province_mapping: String =sparkTool.loader.loadFileString("/sql/dashu/Const_DDL_Hive/idno_province_mapping.sql")
}
