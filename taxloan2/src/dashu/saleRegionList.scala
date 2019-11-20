package dashu

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import util.spark

object saleRegionList {
  def run(hc:HiveContext): Unit = {
//    val conf:SparkConf = new SparkConf().setAppName("saleRegionList")
//    val sc:SparkContext = new SparkContext(conf)
//    val hc:HiveContext = new HiveContext(sc)

    //  sale_region_list_tmp1
    val sale_region_list_tmp1_df:DataFrame = hc.sql(spark.printCmd(constant_full.saleRegionList.sale_region_list_tmp1))
    // hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    sale_region_list_tmp1_df.write.format("orc").mode("overwrite").saveAsTable("sale_region_list_tmp1")
    println("sale_region_list_tmp1计算完成")

    //  创建近2年维度表
    val dim_sellertaxno_latest_2_years_df:DataFrame = hc.sql(spark.printCmd(constant_full.saleRegionList.dim_sellertaxno_latest_2_years))
    // hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    dim_sellertaxno_latest_2_years_df.write.format("orc").mode("overwrite").saveAsTable("dim_sellertaxno_latest_2_years")
    println("dim_sellertaxno_latest_2_years 计算完成")

    //  按商户和交易年份分组，按交易金额降序排列
    val sale_region_list_tmp2_df:DataFrame = hc.sql(spark.printCmd(constant_full.saleRegionList.sale_region_list_tmp2))
    // hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    sale_region_list_tmp2_df.write.format("orc").mode("overwrite").saveAsTable("sale_region_list_tmp2")
    println("sale_region_list_tmp2计算完成")

    //  按商户和交易月份分组，按交易金额降序排列,只需要列出前5个地区
    val sale_region_list_df:DataFrame = hc.sql(spark.printCmd(constant_full.saleRegionList.sale_region_list))
    //hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    sale_region_list_df.write.format("orc").mode("overwrite").saveAsTable("sale_region_list")
    println("sale_region_list计算完成")
  }

}
