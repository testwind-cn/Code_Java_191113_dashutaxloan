package util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import com.plj.scala.tools.{TimeTools, loadString}


object sparkTool {

  private var theNewTime:String=""
  private var theOldTime:String=""
  private var sLastTime:String=""
  private var sLastTime_S:String=""
  private var isFull:Boolean=false
  private var isInit:Boolean=false
  private var hivevarArray:Array[String]=Array[String]()
  private var hiveUseDB:String=""

  def get_theNewTime:String={
    theNewTime
  }

  def get_theOldTime:String={
    theOldTime
  }

  def get_sLastTime:String={
    sLastTime
  }

  def get_sLastTime_S:String={
    sLastTime_S
  }

  def get_isFull:Boolean={
    isFull
  }

  def get_isInit:Boolean={
    isInit
  }

  def runSqlCmd(hc: HiveContext, cmd:String, runSQL:Boolean=false): Unit = {
    println(" ============ 执行命令 Begin =================")
    println(cmd)
    println(" ============ 执行命令 End =================")
    if (runSQL)
      hc.sql(cmd).show(10)
  }

  def printCmd(cmd:String, hc: HiveContext=null, runSQL:Boolean=false): String = {
    println(" ============ 执行命令 Begin =================")
    println(cmd)
    println(" ============ 执行命令 End =================")
    if (runSQL && hc != null )
      hc.sql(cmd).show(10)

    return cmd
  }

  def getTimeArg(args:Array[String],start:String,argPos:Int, addDate:Int=0):String={
    var the_opt: Option[String] = null
    the_opt = args.find(x => x.length>=25 && x.startsWith(start))
    // theOldTime=the_opt.getOrElse(TimeTools.getDateTimeStr(TimeTools.getDate(null,-31))) //"--new=2019-07-20 11:00:00")
    if (the_opt.isDefined)    {
      the_opt.get.substring(argPos)
    } else {
      TimeTools.getDateTimeStr(TimeTools.getDateTime(null,addDate))
    }
  }


  def getLastTimeFromHive(hc: HiveContext=null, table:String="cjlog_last"): Unit = {
    // ***获取上次时间*****************************************************************
    // set hivevar:DATABASE_DEST=dm_taxloan
    // var sss="insert into ${hivevar:DATABASE_DEST}.control_table  select 'cjlog_new' as table_name, from_unixtime(unix_timestamp()) as export_date union all select 'cjlog_last' as table_name, from_unixtime(unix_timestamp()) as export_date"
    // var sss="insert into ${hivevar:DATABASE_DEST}.control_table  select 'cjlog_new' as table_name, '2019-09-25 22:30:00' as export_date union all select 'cjlog_last' as table_name, '2019-09-25 22:30:00' as export_date"
    // sqlContext.sql(sss)
    // 上面两句，可以插入两行数据

    val getLastTime=hc.sql(
      """
select
  date_format( max(export_date),'yyyy-MM-dd HH:mm:ss')
  ,date_format( max(export_date), 'yyyy-MM-dd_HHmmss') as date_s
  ,table_name from ${hivevar:DATABASE_DEST}.control_table where table_name='%s'
group by table_name order by table_name
""".format(table)
    )

    val sArray=getLastTime.take(2)
    sArray.foreach(println)
    sLastTime=sArray(0).get(0).toString.substring(0,19)
    // var sNewTime=sArray(1).get(0).toString.substring(0,19)
    sLastTime_S=sArray(0).get(1).toString.substring(0,17)
    // ***获取上次时间*****************************************************************
  }

  def getLastTimeFromArgs(): Unit = {
    sLastTime=
      TimeTools.getDateTimeStr(TimeTools.getDateTime(theOldTime,0,"yyyy-MM-dd HH:mm:ss"),"yyyy-MM-dd HH:mm:ss")
    sLastTime_S=
      TimeTools.getDateTimeStr(TimeTools.getDateTime(theOldTime,0,"yyyy-MM-dd HH:mm:ss"),"yyyy-MM-dd_HHmmss")
  }


  def getArgs(args:Array[String]):Unit= {

    println("========获取到参数==BEGING=========")
    args.foreach(println)
    println("========获取到参数==END=======")

    var the_opt: Option[String] = null

    the_opt = args.find(x => x.toLowerCase.trim.equals("--init"))
    if (the_opt.isDefined) isInit = true

    the_opt = args.find(x => x.toLowerCase.trim.equals("--full"))
    if (the_opt.isDefined) isFull = true

    the_opt = args.find(x => x.toLowerCase.trim.equals("--incre"))
    if (the_opt.isDefined) isFull = false

    the_opt = args.find(x => x.startsWith("--hiveusedb=") )
      .map(x => x.substring(12))
    hiveUseDB = the_opt.getOrElse("")

    hivevarArray = args
      .filter(x => x.toLowerCase.startsWith("--hivevar:"))
      .map(x => "set " + x.substring(2))
    // set hivevar:DATABASE_DEST=dm_taxloan1

    println("========执行 全量或者 增量=======是否全量:"+isFull.toString)


    theOldTime = getTimeArg(args,"--old=20",6,-31)
    println("==== 旧的时间是："+theOldTime)

    theNewTime = getTimeArg(args,"--new=20",6)
    println("==== 新的时间是："+theNewTime)
  }


  // def runSettings2(hc: SparkSession, cmd :Array[String]): Unit ={
  def runSettings(hc: HiveContext, cmd :Array[String]): Unit ={
    if ( cmd==null || hc == null) {
      println("============ 参数为空 ===========")
      return
    }
    cmd.foreach( x =>
      if ( x.length > 5 ) {
        println("============ 1.设置变量 ===========\n\t" + x )
        println("============ 2.设置变量 ===========")
        hc.sql(x).show
        println("============ 3.设置变量 ===========")
      }
    )
    hivevarArray.foreach( x =>
      {
        println("============ 4.设置命令行参数变量 ===========\n\t" + x)
        hc.sql(x).show
      }
    )
    if ( hiveUseDB.length > 0) {
      println("============ 5.创建默认数据库 ===========\n\t" + "create database if not exists "+hiveUseDB)
      hc.sql("create database if not exists "+hiveUseDB).show
      println("============ 6.设置默认数据库变量 ===========\n\t" + "use "+hiveUseDB)
      hc.sql("use "+hiveUseDB).show
    }

  }

  def getAddTaxno(hc: HiveContext,obj: Object=null):Unit={
    var cmd:String=""
    val add_taxno=loadString.getStringList("add_taxno.txt",obj)
    if ( add_taxno.size > 0 ) {
      println("========= 查询到手动添加商户 =============")
      add_taxno.foreach(println)
      cmd = "set hivevar:ADD_TAXNO=or taxno in (" + add_taxno.map(x => "'" + x + "'").mkString(",") + ")"
      hc.sql(printCmd(cmd)).show()
    } else {
      println("========= 没有手动添加商户 =============")
      cmd = "set hivevar:ADD_TAXNO="
      hc.sql(printCmd(cmd)).show()
    }
    loadString.clearFile("add_taxno.txt",obj)

  }

  def getHiveContext(name:String):HiveContext={
    val conf:SparkConf = new SparkConf().setAppName(name)
    val sc:SparkContext = new SparkContext(conf)
    var hc: HiveContext = new HiveContext(sc)
    return hc
  }
}
