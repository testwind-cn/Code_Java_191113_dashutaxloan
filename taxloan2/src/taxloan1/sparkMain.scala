package taxloan1

import org.apache.spark.sql.hive.HiveContext
import taxloan1.Const.{Const_Common, Const_DDL, Const_counterparty, Const_counterparty_classify, Const_mcht_full_incre, Const_statistics_crossmonth, Const_statistics_month, Const_table_export}
import util.spark

object sparkMain {

  def runInit: Unit = {
// ############################## 日期
    println("=========  测试命令内容  =========")
    println(Const_Common.insert_control_table_mcht_tax)

    val appName: String = "taxloan1_Init"

    val hc:HiveContext = spark.getHiveContext(appName)


    spark.runSettings(hc, Const_Common.global_set_01)

    hc.sql(spark.printCmd(Const_DDL.create_control_table)).show()

    hc.sql(spark.printCmd(Const_DDL.create_cjlog_tmp)).show()
    hc.sql(spark.printCmd(Const_DDL.create_saleinvoice_tmp1)).show()
    hc.sql(spark.printCmd(Const_DDL.create_saleinvoice_tmp2)).show()
    hc.sql(spark.printCmd(Const_DDL.create_saleinvoice_tmp3)).show()
    hc.sql(spark.printCmd(Const_DDL.create_saleinvoice_tmp)).show()
    hc.sql(spark.printCmd(Const_DDL.create_mcht_tax)).show()
    hc.sql(spark.printCmd(Const_DDL.create_latest_month_tmp)).show()
    hc.sql(spark.printCmd(Const_DDL.create_counterparty)).show()
    hc.sql(spark.printCmd(Const_DDL.create_counterparty_classify)).show()
    hc.sql(spark.printCmd(Const_DDL.create_cross_month_tmp)).show()
    hc.sql(spark.printCmd(Const_DDL.create_p2)).show()
    hc.sql(spark.printCmd(Const_DDL.create_statistics_month)).show()
    hc.sql(spark.printCmd(Const_DDL.create_c_p2)).show()
    hc.sql(spark.printCmd(Const_DDL.create_statistics_crossmonth)).show()

    hc.sql(spark.printCmd(Const_DDL.create_counterparty_classify_incre)).show()
    hc.sql(spark.printCmd(Const_DDL.create_counterparty_incre)).show()
    hc.sql(spark.printCmd(Const_DDL.create_mcht_tax_incre)).show()
    hc.sql(spark.printCmd(Const_DDL.create_statistics_crossmonth_incre)).show()
    hc.sql(spark.printCmd(Const_DDL.create_statistics_month_incre)).show()

    hc.sql(spark.printCmd(Const_DDL.create_bak_cjlog_tmp)).show()
    hc.sql(spark.printCmd(Const_DDL.create_bak_cross_month_tmp)).show()
    hc.sql(spark.printCmd(Const_DDL.create_bak_latest_month_tmp)).show()
    hc.sql(spark.printCmd(Const_DDL.create_bak_saleinvoice_tmp)).show()


    /*
    cjlog
    saleinvoice
    saleinvoicedetail
    dim_date
     */
  }

  def runFullIncre(isFull:Boolean): Unit ={

    var cmd : String=null

    println("=========  测试命令内容 B  =========")
    println(Const_Common.insert_control_table_mcht_tax)
    println("=========  测试命令内容 E  =========")

    val appName:String=if ( spark.get_isFull ) "taxloan1_full" else "taxloan1_incre"

    val hc:HiveContext = spark.getHiveContext(appName)

    spark.runSettings(hc, Const_Common.global_set_01)

    spark.setLastTimeFromArgs()
    // spark.setLastTimeFromHive(hc,"cjlog_last")

    cmd="set hivevar:LAST_TIME='" + spark.get_sLastTime + "'"
    hc.sql(spark.printCmd(cmd)).show()

    cmd="set hivevar:LAST_TIME_S='" + spark.get_sLastTime_S + "'"
    hc.sql(spark.printCmd(cmd)).show()

    //    Const_Common.create_cjlog_tmp.sql
    if ( spark.get_isFull ) {
      hc.sql(spark.printCmd(Const_Common.truncate_cjlog_tmp)).show()
      hc.sql(spark.printCmd(Const_mcht_full_incre.insert_cjlog_tmp_full)).show()
    } else {

      hc.sql(spark.printCmd(Const_Common.insert_bak_cjlog_tmp)).show()
      hc.sql(spark.printCmd(Const_Common.truncate_cjlog_tmp)).show()


      // 增加手动跑批的商户号
      spark.getAddTaxno(hc,this)


      hc.sql(spark.printCmd(Const_mcht_full_incre.insert_cjlog_tmp_incre)).show()  // 增量是自己的
      // hc.sql(printCmd(Const_mcht_full_incre.insert_cjlog_tmp_zpp)).show()  // 增量是自己的

    }

    //备份临时表 增量多做这个
    // hc.sql(printCmd(Const_Common.insert_bak_cjlog_tmp)).show()



    //    Const_Common.create_saleinvoice_tmp1.sql

    hc.sql(spark.printCmd(Const_Common.truncate_saleinvoice_tmp1)).show()

    if ( spark.get_isFull ) {
      hc.sql(spark.printCmd(Const_mcht_full_incre.insert_saleinvoice_tmp1_full)).show() // 增量是自己的

    } else {
      hc.sql(spark.printCmd(Const_mcht_full_incre.insert_saleinvoice_tmp1_incre)).show() // 增量是自己的，增量有 distinct 全量没有
    }


    //    Const_Common.create_saleinvoice_tmp2.sql
    hc.sql(spark.printCmd(Const_Common.truncate_saleinvoice_tmp2)).show()


    if ( spark.get_isFull ) {
      hc.sql(spark.printCmd(Const_mcht_full_incre.insert_saleinvoice_tmp2_full)).show()
    } else {
      hc.sql(spark.printCmd(Const_mcht_full_incre.insert_saleinvoice_tmp2_incre)).show()  // 增量是自己的，增量有 distinct 全量没有
    }

    if ( ! spark.get_isFull ) //备份临时表
      hc.sql(spark.printCmd(Const_Common.insert_bak_saleinvoice_tmp)).show()


    //    Const_Common.create_saleinvoice_tmp.sql
    hc.sql(spark.printCmd(Const_Common.truncate_saleinvoice_tmp)).show()

    hc.sql(spark.printCmd(Const_mcht_full_incre.insert_saleinvoice_tmp)).show() // 增量是自己的,但和全量一样

    //备份临时表 增量多做这个
    // hc.sql(printCmd(Const_Common.insert_bak_saleinvoice_tmp)).show()

    println("=========  插入saleinvoice_tmp成功  =========")

    /*****************************************商户表********************************************/

    if ( spark.get_isFull ) {
      hc.sql(spark.printCmd(Const_Common.truncate_mcht_tax)).show() // 增量不做这步
    }

    hc.sql(spark.printCmd(Const_mcht_full_incre.insert_mcht_tax)).show()  // 增量是自己的,但和全量一样

    /*****************************************交易对手表********************************************/

    if ( ! spark.get_isFull ) //备份临时表
      hc.sql(spark.printCmd(Const_Common.insert_bak_latest_month_tmp)).show()
    //增量多做一个：备份临时表  hc.sql(Const_Common.insert_bak_latest_montn_tmp)

    hc.sql(spark.printCmd(Const_Common.truncate_latest_month_tmp)).show()

    hc.sql(spark.printCmd(Const_counterparty.insert_latest_month_tmp)).show()

    if ( spark.get_isFull || true ) {
      hc.sql(spark.printCmd(Const_Common.truncate_counterparty)).show() // 增量不做这步
    }

    hc.sql(spark.printCmd(Const_counterparty.insert_counterparty)).show()

    println("=========  交易对手表插入成功  =========")

    /*****************************************交易对手分类表****************************************/

    if ( spark.get_isFull || true ) {
      hc.sql(spark.printCmd(Const_Common.truncate_counterparty_classify)).show() // 增量不做这步
    }


    hc.sql(spark.printCmd(Const_counterparty_classify.insert_counterparty_classify)).show()





    /*****************************************月统计表********************************************/


    if ( ! spark.get_isFull ) //备份临时表
      hc.sql(spark.printCmd(Const_Common.insert_bak_cross_month_tmp)).show()

    //    Const_Common.create_cross_month_tmp
    hc.sql(spark.printCmd(Const_Common.truncate_cross_month_tmp)).show()


    if ( spark.get_isFull ) {
      hc.sql(spark.printCmd(Const_mcht_full_incre.insert_cross_month_tmp_full)).show() // 增量全量使用不同的,使用各自的
    } else {
      hc.sql(spark.printCmd(Const_mcht_full_incre.insert_cross_month_tmp_incre)).show() // 增量全量使用不同的,使用各自的
    }

    println("=========  cross_month_tmp插入成功  =========")


    //备份临时表
    // 增量多做一次备份 hc.sql(Const_Common.insert_bak_cross_montn_tmp)



    //  Create
    hc.sql(spark.printCmd(Const_Common.truncate_p2)).show()

    hc.sql(spark.printCmd(Const_statistics_month.insert_p2)).show()


    println("=========  p2插入完成  =========")



    if ( spark.get_isFull || true) {  // 增量不做这步
      hc.sql(spark.printCmd(Const_Common.truncate_statistics_month)).show()  // 增量不做这步
    }

    hc.sql(spark.printCmd(Const_statistics_month.insert_statistics_month)).show()

    println("=========  月统计表插入成功  =========")


    /*****************************************跨月统计表********************************************/

    hc.sql(spark.printCmd(Const_Common.truncate_c_p2)).show()

    hc.sql(spark.printCmd(Const_statistics_crossmonth.insert_c_p2)).show()



    println("=========  c_p2成功  =========")

    if ( spark.get_isFull || true ) {  // 增量不做这步
      hc.sql(spark.printCmd(Const_Common.truncate_statistics_crossmonth)).show()
    }

    hc.sql(spark.printCmd(Const_statistics_crossmonth.insert_statistics_crossmonth)).show()

    println("=========  跨月统计表插入完成  =========")


    if ( ! isFull ) {
      /**************************************更新增量导出表********************************************/

      hc.sql(spark.printCmd(Const_table_export.insert_counterparty_incre)).show()
      printf("***********************交易对手表增量数据导出到增量辅助表完成************************")

      hc.sql(spark.printCmd(Const_table_export.insert_counterparty_classify_incre)).show()
      printf("***********************交易对手分类表增量数据导出到增量辅助表完成************************")

      hc.sql(spark.printCmd(Const_table_export.insert_statistics_month_incre)).show()
      printf("***********************月统计表增量数据导出到增量辅助表完成************************")

      hc.sql(spark.printCmd(Const_table_export.insert_statistics_crossmonth_incre)).show()
      printf("***********************跨月统计表增量数据导出到增量辅助表完成************************")

      hc.sql(spark.printCmd(Const_table_export.insert_mcht_tax_incre)).show()
      printf("***********************商户表增量数据导出到增量辅助表完成************************")

    }


    /**************************************更新增量导出控制表时间挫*************************************/



    hc.sql(spark.printCmd(Const_Common.insert_control_table_mcht_tax)).show()
    hc.sql(spark.printCmd(Const_Common.insert_control_table_counterparty)).show()
    hc.sql(spark.printCmd(Const_Common.insert_control_table_counterparty_classify)).show()
    hc.sql(spark.printCmd(Const_Common.insert_control_table_statistics_month)).show()
    hc.sql(spark.printCmd(Const_Common.insert_control_table_statistics_crossmonth)).show()


    /******** 改到 Sqoop 结束后做更新 *******************/
    /*
    cmd="insert into ${hivevar:DATABASE_DEST}.control_table  select 'cjlog_last' as table_name, '" +  theNewTime + "' as export_date"
    hc.sql(spark.printCmd(cmd)).show()
     */
  }

  def main(args:Array[String]):Unit = {

    spark.getArgs(args)

    if (spark.get_isInit) {
      println("========执行初始化=======")
      runInit
      return
    }

    println("========执行 全量或者 增量=======是否全量:"+spark.get_isFull.toString)

    runFullIncre(spark.get_isFull)
  }

}
