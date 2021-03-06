package dashu.constant_full
import util.sparkTool

object saleinvoice_month {
  //salesInvoiceInfo_tmp1_24month 筛选近24个月的数据
  val salesInvoiceInfo_tmp1_24month:String =sparkTool.loader.loadFileString("/sql/dashu/saleinvoice_month/salesInvoiceInfo_tmp1_24month.sql")

  //salesInvoiceInfo_tmp1_36month 筛选近36个月的数据
  val salesInvoiceInfo_tmp1_36month:String =sparkTool.loader.loadFileString("/sql/dashu/saleinvoice_month/salesInvoiceInfo_tmp1_36month.sql")

  //生成sellertaxno_24month商户最近24month维度表
  val dim_sellertaxno_24month:String =sparkTool.loader.loadFileString("/sql/dashu/saleinvoice_month/dim_sellertaxno_24month.sql")


  //生成sellertaxno_36month商户最近36month维度表
  val dim_sellertaxno_36month:String =sparkTool.loader.loadFileString("/sql/dashu/saleinvoice_month/dim_sellertaxno_36month.sql")

  //关联36个月的维度表为计算同比、环比等指标
  val salesInvoiceInfo_tmp1_36month_dim:String =sparkTool.loader.loadFileString("/sql/dashu/saleinvoice_month/salesInvoiceInfo_tmp1_36month_dim.sql")

  //--关联维度表并计算除同比环比以外的所有其它近24个月的指标
  val salesInvoiceInfo_tmp1_24month_dim:String =sparkTool.loader.loadFileString("/sql/dashu/saleinvoice_month/salesInvoiceInfo_tmp1_24month_dim.sql")

  //利用分析函数计算每个月前一个月和前一年的应税销售额
  val salesInvoiceInfo_tmp1_36month_dim_lag:String =sparkTool.loader.loadFileString("/sql/dashu/saleinvoice_month/salesInvoiceInfo_tmp1_36month_dim_lag.sql")

  //--生成最终结果表saleinvoice_month
  val saleinvoice_month:String =sparkTool.loader.loadFileString("/sql/dashu/saleinvoice_month/saleinvoice_month.sql")

  val insertSql:String =sparkTool.loader.loadFileString("/sql/dashu/saleinvoice_month/insertSql.sql")

}
