package dashu2
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext


object sale_region_list {
  def main(args: Array[String]): Unit = {
    val conf:SparkConf = new SparkConf().setAppName("sale_region_list")
    val sc:SparkContext = new SparkContext(conf)
    val hc:HiveContext = new HiveContext(sc)

    //  sale_region_list_tmp1
    val sale_region_list_tmp1_df:DataFrame = hc.sql(constant_full_sql.saleRegionListSql.sale_region_list_tmp1)
    hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    sale_region_list_tmp1_df.write.format("orc").mode("overwrite").saveAsTable("sale_region_list_tmp1")
    println("sale_region_list_tmp1计算完成")

    //  创建近2年维度表
    val dim_sellertaxno_latest_2_years_df:DataFrame = hc.sql(constant_full_sql.saleRegionListSql.dim_sellertaxno_latest_2_years)
    hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    dim_sellertaxno_latest_2_years_df.write.format("orc").mode("overwrite").saveAsTable("dim_sellertaxno_latest_2_years")
    println("dim_sellertaxno_latest_2_years 计算完成")

    //  按商户和交易年份分组，按交易金额降序排列
    val sale_region_list_tmp2_df:DataFrame = hc.sql(constant_full_sql.saleRegionListSql.sale_region_list_tmp2)
    hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    sale_region_list_tmp2_df.write.format("orc").mode("overwrite").saveAsTable("sale_region_list_tmp2")
    println("sale_region_list_tmp2计算完成")

    //  按商户和交易年份分组，按交易金额降序排列,只需要列出前5个地区
    val sale_region_list_df:DataFrame = hc.sql(constant_full_sql.saleRegionListSql.sale_region_list)
    // 注册DataFrame为临时表
    sale_region_list_df.registerTempTable("tmp_sale_region_list")
    // 保存临时表到hive
    hc.sql(constant_full_sql.saleRegionListSql.insertSql)
    println("sale_region_list计算完成")
  }
}
