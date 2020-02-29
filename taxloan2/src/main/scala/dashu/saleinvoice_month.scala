package dashu

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import util.sparkTool

object saleinvoice_month {

  def run(hc:HiveContext): Unit = {
//    val conf:SparkConf = new SparkConf().setAppName("saleinvoice_month")
//    val sc:SparkContext = new SparkContext(conf)
//    val hc:HiveContext = new HiveContext(sc)


    //salesInvoiceInfo_tmp1_24month 筛选近24个月的数据
    val salesInvoiceInfo_tmp1_24month_df:DataFrame = hc.sql(sparkTool.printCmd(constant_full.saleinvoice_month.salesInvoiceInfo_tmp1_24month))
    // hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    salesInvoiceInfo_tmp1_24month_df.write.format("orc").mode("overwrite").saveAsTable("salesInvoiceInfo_tmp1_24month")
    println("salesInvoiceInfo_tmp1_24month 计算完成")


    //salesInvoiceInfo_tmp1_36month 筛选近36个月的数据
    val salesInvoiceInfo_tmp1_36month_df:DataFrame = hc.sql(sparkTool.printCmd(constant_full.saleinvoice_month.salesInvoiceInfo_tmp1_36month))
    // hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    salesInvoiceInfo_tmp1_36month_df.write.format("orc").mode("overwrite").saveAsTable("salesInvoiceInfo_tmp1_36month")
    println("salesInvoiceInfo_tmp1_36month 计算完成")



    //  //生成dim_sellertaxno_24month商户最近24month维度表
    val dim_sellertaxno_24month_df:DataFrame = hc.sql(sparkTool.printCmd(constant_full.saleinvoice_month.dim_sellertaxno_24month))
    // hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    dim_sellertaxno_24month_df.write.format("orc").mode("overwrite").saveAsTable("dim_sellertaxno_24month")
    println("dim_sellertaxno_24month 计算完成")

    //  //生成dim_sellertaxno_36month商户最近36month维度表
    val dim_sellertaxno_36month_df:DataFrame = hc.sql(sparkTool.printCmd(constant_full.saleinvoice_month.dim_sellertaxno_36month))
    // hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    dim_sellertaxno_36month_df.write.format("orc").mode("overwrite").saveAsTable("dim_sellertaxno_36month")
    println("dim_sellertaxno_36month 计算完成")

    //salesInvoiceInfo_tmp1_36month_dim关联36个月的维度表为计算同比、环比等指标
    val salesInvoiceInfo_tmp1_36month_dim_df:DataFrame = hc.sql(sparkTool.printCmd(constant_full.saleinvoice_month.salesInvoiceInfo_tmp1_36month_dim))
    // hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    salesInvoiceInfo_tmp1_36month_dim_df.write.format("orc").mode("overwrite").saveAsTable("salesInvoiceInfo_tmp1_36month_dim")
    println("salesInvoiceInfo_tmp1_36month_dim 计算完成")

    //  salesInvoiceInfo_tmp1_24month_dim 关联维度表并计算除同比环比以外的所有其它近24个月的指标
    val salesInvoiceInfo_tmp1_24month_dim_df:DataFrame = hc.sql(sparkTool.printCmd(constant_full.saleinvoice_month.salesInvoiceInfo_tmp1_24month_dim))
    // hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    salesInvoiceInfo_tmp1_24month_dim_df.write.format("orc").mode("overwrite").saveAsTable("salesInvoiceInfo_tmp1_24month_dim")
    println("salesInvoiceInfo_tmp1_24month_dim 计算完成")

    //  利用分析函数计算每个月前一个月和前一年的应税销售额
    val salesInvoiceInfo_tmp1_36month_dim_lag_df:DataFrame = hc.sql(sparkTool.printCmd(constant_full.saleinvoice_month.salesInvoiceInfo_tmp1_36month_dim_lag))
    // hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    salesInvoiceInfo_tmp1_36month_dim_lag_df.write.format("orc").mode("overwrite").saveAsTable("salesInvoiceInfo_tmp1_36month_dim_lag")
    println("salesInvoiceInfo_tmp1_36month_dim_lag 计算完成")


    //  生成最终结果表 saleinvoice_month
    val saleinvoice_month_df:DataFrame = hc.sql(sparkTool.printCmd(constant_full.saleinvoice_month.saleinvoice_month))
    // hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    saleinvoice_month_df.write.format("orc").mode("overwrite").saveAsTable("saleinvoice_month")
    println("saleinvoice_month 计算完成")

  }
}
