package dashu.constant_full
import util.sparkTool


object saleRegionList {
  val sale_region_list_tmp1:String =sparkTool.loader.loadFileString("/sql/dashu/saleRegionList/sale_region_list_tmp1.sql")

  val dim_sellertaxno_latest_2_years:String =sparkTool.loader.loadFileString("/sql/dashu/saleRegionList/dim_sellertaxno_latest_2_years.sql")

  val sale_region_list_tmp2:String =sparkTool.loader.loadFileString("/sql/dashu/saleRegionList/sale_region_list_tmp2.sql")

  val sale_region_list:String =sparkTool.loader.loadFileString("/sql/dashu/saleRegionList/sale_region_list.sql")

  val insertSql:String =sparkTool.loader.loadFileString("/sql/dashu/saleRegionList/insertSql.sql")

}
