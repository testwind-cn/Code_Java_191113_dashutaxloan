package dashu.constant_full
import util.sparkTool

object saleinvoice_tmp {
  val cjlog_tmp:String =sparkTool.loader.getString("/sql/dashu/saleinvoice_tmp/cjlog_tmp.sql")
  val cjlog_tmp_full:String =sparkTool.loader.getString("/sql/dashu/saleinvoice_tmp/cjlog_tmp_full.sql")

  val truncate_cjlog_tmp:String =sparkTool.loader.getString("/sql/dashu/saleinvoice_tmp/truncate_cjlog_tmp.sql")

  val insert_cjlog_tmp:String =sparkTool.loader.getString("/sql/dashu/saleinvoice_tmp/insert_cjlog_tmp.sql")

  val truncate_saleinvoice_tmp:String =sparkTool.loader.getString("/sql/dashu/saleinvoice_tmp/truncate_saleinvoice_tmp.sql")

  val insert_saleinvoice_tmp:String =sparkTool.loader.getString("/sql/dashu/saleinvoice_tmp/insert_saleinvoice_tmp.sql")

  val truncate_saleinvoice_tmp1:String =sparkTool.loader.getString("/sql/dashu/saleinvoice_tmp/truncate_saleinvoice_tmp1.sql")

  val insert_saleinvoice_tmp1:String =sparkTool.loader.getString("/sql/dashu/saleinvoice_tmp/insert_saleinvoice_tmp1.sql")

  val truncate_saleinvoice_tmp2:String =sparkTool.loader.getString("/sql/dashu/saleinvoice_tmp/truncate_saleinvoice_tmp2.sql")

  val insert_saleinvoice_tmp2:String =sparkTool.loader.getString("/sql/dashu/saleinvoice_tmp/insert_saleinvoice_tmp2.sql")

  val truncate_saleinvoice_tmp3:String =sparkTool.loader.getString("/sql/dashu/saleinvoice_tmp/truncate_saleinvoice_tmp3.sql")

  val insert_saleinvoice_tmp3:String =sparkTool.loader.getString("/sql/dashu/saleinvoice_tmp/insert_saleinvoice_tmp3.sql")


  //处理oldtaxno为空或者null的记录
  val saleinvoice_tmp1:String =sparkTool.loader.getString("/sql/dashu/saleinvoice_tmp/saleinvoice_tmp1.sql")

  //处理oldtaxno!='null'，且oldtaxno!=''的发票数据，合并新老税号数据
  val saleinvoice_tmp2:String =sparkTool.loader.getString("/sql/dashu/saleinvoice_tmp/saleinvoice_tmp2.sql")


  val saleinvoice_tmp3:String =sparkTool.loader.getString("/sql/dashu/saleinvoice_tmp/saleinvoice_tmp3.sql")

  //过滤重复的发票数据
  val saleinvoice_tmp:String =sparkTool.loader.getString("/sql/dashu/saleinvoice_tmp/saleinvoice_tmp.sql")
}
