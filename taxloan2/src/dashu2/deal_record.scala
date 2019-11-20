package dashu2

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext


object deal_record {
  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("deal_record")
    val sc:SparkContext = new SparkContext(conf)
    val hc:HiveContext = new HiveContext(sc)

    //  近6个月最大连续未开票间隔天数（销项）
    val nonDealDaysL6_df:DataFrame = hc.sql(constant_full_sql.dealRecordInfoSql.nonDealDaysL6)
    hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    nonDealDaysL6_df.write.format("orc").mode("overwrite").saveAsTable("non_deal_days_l6")
    println("近6个月最大连续未开票间隔天数计算完成")
    //  近12个月最大连续未开票间隔天数（销项）
    val nonDealDaysL12_df:DataFrame = hc.sql(constant_full_sql.dealRecordInfoSql.nonDealDaysL12)
    hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    nonDealDaysL12_df.write.format("orc").mode("overwrite").saveAsTable("non_deal_days_l12")
    println("近12个月最大连续未开票间隔天数计算完成")
    //  近24个月最大连续未开票间隔天数（销项）
    val nonDealDaysL24_df:DataFrame = hc.sql(constant_full_sql.dealRecordInfoSql.nonDealDaysL24)
    hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    nonDealDaysL24_df.write.format("orc").mode("overwrite").saveAsTable("non_deal_days_l24")
    println("近24个月最大连续未开票间隔天数计算完成")

    //  交易记录信息目标表前半部分
    val dealRecordInfoPrehalf_df:DataFrame = hc.sql(constant_full_sql.dealRecordInfoSql.dealRecordInfoPrehalf)
    hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    dealRecordInfoPrehalf_df.write.format("orc").mode("overwrite").saveAsTable("deal_record_info_prehalf")
    println("交易记录信息目标表前半部分计算完成")

    //  生成交易记录信息表
    val deal_record_df:DataFrame = hc.sql(constant_full_sql.dealRecordInfoSql.dealRecordInfoGenerate)
    // 注册DataFrame为临时表
    deal_record_df.registerTempTable("tmp_deal_record")
    // 保存临时表到hive
    hc.sql(constant_full_sql.dealRecordInfoSql.insertSql)
    println("交易记录信息表计算完成")
  }
}
