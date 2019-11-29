package dashu

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import util.sparkTool


//生成交易记录信息目标表
object dealRecordInfo {
  def run(hc:HiveContext): Unit = {

  //  val conf:SparkConf = new SparkConf().setAppName("dealRecordInfo")
  //  val sc:SparkContext = new SparkContext(conf)
  //  val hc:HiveContext = new HiveContext(sc)

    //  近6个月最大连续未开票间隔天数（销项）
    val nonDealDaysL6_df:DataFrame = hc.sql(sparkTool.printCmd(constant_full.dealRecordInfo.nonDealDaysL6))

    // hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    nonDealDaysL6_df.write.format("orc").mode("overwrite").saveAsTable("non_deal_days_l6")
    println("近6个月最大连续未开票间隔天数计算完成")
    //  近12个月最大连续未开票间隔天数（销项）
    val nonDealDaysL12_df:DataFrame = hc.sql(sparkTool.printCmd(constant_full.dealRecordInfo.nonDealDaysL12))

    //hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    nonDealDaysL12_df.write.format("orc").mode("overwrite").saveAsTable("non_deal_days_l12")
    println("近12个月最大连续未开票间隔天数计算完成")
    //  近24个月最大连续未开票间隔天数（销项）
    val nonDealDaysL24_df:DataFrame = hc.sql(sparkTool.printCmd(constant_full.dealRecordInfo.nonDealDaysL24))
    // hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    nonDealDaysL24_df.write.format("orc").mode("overwrite").saveAsTable("non_deal_days_l24")
    println("近24个月最大连续未开票间隔天数计算完成")

    //  交易记录信息目标表前半部分
    val dealRecordInfoPrehalf_df:DataFrame = hc.sql(sparkTool.printCmd(constant_full.dealRecordInfo.dealRecordInfoPrehalf))
    //hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    dealRecordInfoPrehalf_df.write.format("orc").mode("overwrite").saveAsTable("deal_record_info_prehalf")
    println("交易记录信息目标表前半部分计算完成")

    //  生成交易记录信息表
    val dealRecordInfo_df:DataFrame = hc.sql(sparkTool.printCmd(constant_full.dealRecordInfo.dealRecordInfoGenerate))
    //hc.sql(s"use ${constant_common.Const_common.DATABASE}")
    dealRecordInfo_df.write.format("orc").mode("overwrite").saveAsTable("deal_record")
    println("交易记录信息表计算完成")
  }

}
