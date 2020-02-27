package dashu.constant_full
import com.plj.tools.scala.loadString

object saleinvoice_tmp {
  val cjlog_tmp:String =loadString.getString("/sql/dashu/saleinvoice_tmp/cjlog_tmp.sql",this)
  val cjlog_tmp_full:String =loadString.getString("/sql/dashu/saleinvoice_tmp/cjlog_tmp_full.sql",this)

  val truncate_cjlog_tmp:String =loadString.getString("/sql/dashu/saleinvoice_tmp/truncate_cjlog_tmp.sql",this)

  val insert_cjlog_tmp:String =loadString.getString("/sql/dashu/saleinvoice_tmp/insert_cjlog_tmp.sql",this)

  val truncate_saleinvoice_tmp:String =loadString.getString("/sql/dashu/saleinvoice_tmp/truncate_saleinvoice_tmp.sql",this)

  val insert_saleinvoice_tmp:String =loadString.getString("/sql/dashu/saleinvoice_tmp/insert_saleinvoice_tmp.sql",this)

  val truncate_saleinvoice_tmp1:String =loadString.getString("/sql/dashu/saleinvoice_tmp/truncate_saleinvoice_tmp1.sql",this)

  val insert_saleinvoice_tmp1:String =loadString.getString("/sql/dashu/saleinvoice_tmp/insert_saleinvoice_tmp1.sql",this)

  val truncate_saleinvoice_tmp2:String =loadString.getString("/sql/dashu/saleinvoice_tmp/truncate_saleinvoice_tmp2.sql",this)

  val insert_saleinvoice_tmp2:String =loadString.getString("/sql/dashu/saleinvoice_tmp/insert_saleinvoice_tmp2.sql",this)

  val truncate_saleinvoice_tmp3:String =loadString.getString("/sql/dashu/saleinvoice_tmp/truncate_saleinvoice_tmp3.sql",this)

  val insert_saleinvoice_tmp3:String =loadString.getString("/sql/dashu/saleinvoice_tmp/insert_saleinvoice_tmp3.sql",this)


  //处理oldtaxno为空或者null的记录
  val saleinvoice_tmp1:String =loadString.getString("/sql/dashu/saleinvoice_tmp/saleinvoice_tmp1.sql",this)

  //处理oldtaxno!='null'，且oldtaxno!=''的发票数据，合并新老税号数据
  val saleinvoice_tmp2:String =loadString.getString("/sql/dashu/saleinvoice_tmp/saleinvoice_tmp2.sql",this)


  val saleinvoice_tmp3:String =loadString.getString("/sql/dashu/saleinvoice_tmp/saleinvoice_tmp3.sql",this)

  //过滤重复的发票数据
  val saleinvoice_tmp:String =loadString.getString("/sql/dashu/saleinvoice_tmp/saleinvoice_tmp.sql",this)
}
