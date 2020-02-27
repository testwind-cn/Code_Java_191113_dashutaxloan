package dashu.constant_full
import com.plj.scala.tools.loadString


object saleRegionList {
  val sale_region_list_tmp1:String =loadString.getString("/sql/dashu/saleRegionList/sale_region_list_tmp1.sql",this)

  val dim_sellertaxno_latest_2_years:String =loadString.getString("/sql/dashu/saleRegionList/dim_sellertaxno_latest_2_years.sql",this)

  val sale_region_list_tmp2:String =loadString.getString("/sql/dashu/saleRegionList/sale_region_list_tmp2.sql",this)

  val sale_region_list:String =loadString.getString("/sql/dashu/saleRegionList/sale_region_list.sql",this)

  val insertSql:String =loadString.getString("/sql/dashu/saleRegionList/insertSql.sql",this)

}
