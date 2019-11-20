package dashu

import org.apache.spark.sql.hive.HiveContext
import util.spark

object taxloan {

  def runInit: Unit = {

  }

  def runFullIncre(isFull:Boolean): Unit = {

    var cmd: String = null

    val appName:String=if ( isFull ) "dashu_full" else "dashu_incre"

    val hc:HiveContext = spark.getHiveContext(appName)

    spark.runSettings(hc, dashu.constant_common.Const_common.global_set_01)

    spark.setLastTimeFromArgs()
    // spark.setLastTimeFromHive(hc)

    cmd="set hivevar:LAST_TIME='" + spark.get_sLastTime + "'"
    hc.sql(spark.printCmd(cmd)).show()

    cmd="set hivevar:LAST_TIME_S='" + spark.get_sLastTime_S + "'"
    hc.sql(spark.printCmd(cmd)).show()

    saleinvoice_tmp.run(hc)
    saleRegionList.run(hc)
    dealRecordInfo.run(hc)
    downCustomerList.run(hc)
    saleinvoice_month.run(hc)
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
