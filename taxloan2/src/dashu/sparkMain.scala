package dashu

import org.apache.spark.sql.hive.HiveContext
import util.sparkTool

object sparkMain {

  def runInit(hc:HiveContext): Unit = {
    hc.sql(sparkTool.printCmd(dashu.constant_common.Const_common.create_dim_db)).show()
    hc.sql(sparkTool.printCmd(dashu.constant_common.Const_common.create_dim_date)).show()
    hc.sql(sparkTool.printCmd(dashu.constant_common.Const_common.idno_area_mapping)).show()
    hc.sql(sparkTool.printCmd(dashu.constant_common.Const_common.idno_province_mapping)).show()
  }

  def runFullIncre(hc:HiveContext,isFull:Boolean): Unit ={

    var cmd: String = null

    sparkTool.getLastTimeFromArgs()
    // spark.getLastTimeFromHive(hc,"cjlog_last_"+this.getClass.getPackage.getName)

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

    val appName:String=
      if (sparkTool.get_isInit)
        "dashu_init"
      else if ( sparkTool.get_isFull )
        "dashu_full"
      else
        "dashu_incre"

    val hc:HiveContext = sparkTool.getHiveContext(appName)

    sparkTool.runSettings(hc, dashu.constant_common.Const_common.global_set_01)

    if (sparkTool.get_isInit) {
      println("========只执行初始化=======")
      runInit(hc)
    } else {
      // println("========先执行初始化=======")
      // runInit(hc)
      println("========执行 全量或者 增量=======是否全量:"+sparkTool.get_isFull.toString)
      runFullIncre(hc,sparkTool.get_isFull)
    }
  }


}
