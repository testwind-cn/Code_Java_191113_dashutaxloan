package dashu.constant_full
import util.sparkTool

object downCustomerList {
  val down_customer_list_tmp1:String =sparkTool.loader.getString("/sql/dashu/downCustomerList/down_customer_list_tmp1.sql")

  val down_customer_list_tmp2:String =sparkTool.loader.getString("/sql/dashu/downCustomerList/down_customer_list_tmp2.sql")

  val down_customer_list_tmp3:String =sparkTool.loader.getString("/sql/dashu/downCustomerList/down_customer_list_tmp3.sql")

  val down_customer_list_tmp4:String =sparkTool.loader.getString("/sql/dashu/downCustomerList/down_customer_list_tmp4.sql")

  val dim_quarter:String =sparkTool.loader.getString("/sql/dashu/downCustomerList/dim_quarter.sql")

  val dim_sellertaxno_quarter_24month:String =sparkTool.loader.getString("/sql/dashu/downCustomerList/dim_sellertaxno_quarter_24month.sql")

  val down_customer_list:String =sparkTool.loader.getString("/sql/dashu/downCustomerList/down_customer_list.sql")

  val insertSql:String =sparkTool.loader.getString("/sql/dashu/downCustomerList/insertSql.sql")
}
