package dashu.constant_full
import com.plj.scala.tools.loadString

object downCustomerList {
  val down_customer_list_tmp1:String =loadString.getString("/sql/dashu/downCustomerList/down_customer_list_tmp1.sql",this)

  val down_customer_list_tmp2:String =loadString.getString("/sql/dashu/downCustomerList/down_customer_list_tmp2.sql",this)

  val down_customer_list_tmp3:String =loadString.getString("/sql/dashu/downCustomerList/down_customer_list_tmp3.sql",this)

  val down_customer_list_tmp4:String =loadString.getString("/sql/dashu/downCustomerList/down_customer_list_tmp4.sql",this)

  val dim_quarter:String =loadString.getString("/sql/dashu/downCustomerList/dim_quarter.sql",this)

  val dim_sellertaxno_quarter_24month:String =loadString.getString("/sql/dashu/downCustomerList/dim_sellertaxno_quarter_24month.sql",this)

  val down_customer_list:String =loadString.getString("/sql/dashu/downCustomerList/down_customer_list.sql",this)

  val insertSql:String =loadString.getString("/sql/dashu/downCustomerList/insertSql.sql",this)
}
