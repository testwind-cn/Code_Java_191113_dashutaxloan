package dashu

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import util.sparkTool


object downCustomerList {
  def run(hc:HiveContext): Unit = {
    //val conf:SparkConf = new SparkConf().setAppName("downCustomerList")
    //val sc:SparkContext = new SparkContext(conf)
    //val hc:HiveContext = new HiveContext(sc)

    //  down_customer_list_tmp1 按商户号、季度数分组
    val down_customer_list_tmp1_df:DataFrame = hc.sql(sparkTool.printCmd(constant_full.downCustomerList.down_customer_list_tmp1))
    //hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    down_customer_list_tmp1_df.write.format("orc").mode("overwrite").saveAsTable("down_customer_list_tmp1")
    println("down_customer_list_tmp1 计算完成")

    //  down_customer_list_tmp2 按商户号、季度数、交易对手分组
    val down_customer_list_tmp2_df:DataFrame = hc.sql(sparkTool.printCmd(constant_full.downCustomerList.down_customer_list_tmp2))
    //hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    down_customer_list_tmp2_df.write.format("orc").mode("overwrite").saveAsTable("down_customer_list_tmp2")
    println("down_customer_list_tmp2 计算完成")

    //  down_customer_list_tmp3 对down_customer_list_tmp2取出前5大交易客户并对前5大客户交易记录进行计算
    val down_customer_list_tmp3_df:DataFrame = hc.sql(sparkTool.printCmd(constant_full.downCustomerList.down_customer_list_tmp3))
    //hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    down_customer_list_tmp3_df.write.format("orc").mode("overwrite").saveAsTable("down_customer_list_tmp3")
    println("down_customer_list_tmp3 计算完成")

    //  down_customer_list_tmp4 根据down_customer_list_tmp3计算不带全维度的目标表
    val down_customer_list_tmp4_df:DataFrame = hc.sql(sparkTool.printCmd(constant_full.downCustomerList.down_customer_list_tmp4))
    //hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    down_customer_list_tmp4_df.write.format("orc").mode("overwrite").saveAsTable("down_customer_list_tmp4")
    println("down_customer_list_tmp4 计算完成")

    //  根据日期维度表生成季度维度表
    val dim_quarter_df:DataFrame = hc.sql(sparkTool.printCmd(constant_full.downCustomerList.dim_quarter))
    //hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    dim_quarter_df.write.format("orc").mode("overwrite").saveAsTable("dim_quarter")
    println("dim_quarter 计算完成")

    //创建卖方税号、季度维度表
    val dim_sellertaxno_quarter_24month_df:DataFrame = hc.sql(sparkTool.printCmd(constant_full.downCustomerList.dim_sellertaxno_quarter_24month))
    //hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    dim_sellertaxno_quarter_24month_df.write.format("orc").mode("overwrite").saveAsTable("dim_sellertaxno_quarter_24month")
    println("dim_sellertaxno_quarter_24month 计算完成")

    //  down_customer_list_tmp4关联税号、季度维度表生成全维度的目标表down_customer_list
    val down_customer_list_df:DataFrame = hc.sql(sparkTool.printCmd(constant_full.downCustomerList.down_customer_list))
    //hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    down_customer_list_df.write.format("orc").mode("overwrite").saveAsTable("down_customer_list")
    println("down_customer_list 计算完成")



  }
}
