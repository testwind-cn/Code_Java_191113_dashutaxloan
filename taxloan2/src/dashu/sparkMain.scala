package dashu

import org.apache.spark.sql.hive.HiveContext
import util.sparkTool

object sparkMain {

  def runInit: Unit = {

  }

  def runFullIncre(isFull:Boolean): Unit = {

    var cmd: String = null

    val appName:String=if ( isFull ) "dashu_full" else "dashu_incre"

    val hc:HiveContext = sparkTool.getHiveContext(appName)

    sparkTool.runSettings(hc, dashu.constant_common.Const_common.global_set_01)

    sparkTool.setLastTimeFromArgs()
    // spark.setLastTimeFromHive(hc)

    cmd="set hivevar:LAST_TIME='" + sparkTool.get_sLastTime + "'"
    hc.sql(sparkTool.printCmd(cmd)).show()

    cmd="set hivevar:LAST_TIME_S='" + sparkTool.get_sLastTime_S + "'"
    hc.sql(sparkTool.printCmd(cmd)).show()

    saleinvoice_tmp.run(hc,true)
    saleRegionList.run(hc)
    dealRecordInfo.run(hc)
    downCustomerList.run(hc)
    saleinvoice_month.run(hc)
  }


  def main(args:Array[String]):Unit = {

    sparkTool.getArgs(args)

    if (sparkTool.get_isInit) {
      println("========执行初始化=======")
      runInit
      return
    }

    println("========执行 全量或者 增量=======是否全量:"+sparkTool.get_isFull.toString)

    runFullIncre(sparkTool.get_isFull)
  }


}
