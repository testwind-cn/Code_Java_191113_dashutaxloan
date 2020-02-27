package dashu.constant_full
import com.plj.tools.scala.loadString

object dealRecordInfo {

  //近6个月最大连续未开票间隔天数（销项）
  val nonDealDaysL6:String =loadString.getString("/sql/dashu/dealRecordInfo/nonDealDaysL6.sql",this)

  //近12个月最大连续未开票间隔天数（销项）
  val nonDealDaysL12:String =loadString.getString("/sql/dashu/dealRecordInfo/nonDealDaysL12.sql",this)

  //近24个月最大连续未开票间隔天数（销项）
  val nonDealDaysL24:String =loadString.getString("/sql/dashu/dealRecordInfo/nonDealDaysL24.sql",this)

  //易记录信息目标表前半部分
  val dealRecordInfoPrehalf:String =loadString.getString("/sql/dashu/dealRecordInfo/dealRecordInfoPrehalf.sql",this)

  val dealRecordInfoGenerate:String =loadString.getString("/sql/dashu/dealRecordInfo/dealRecordInfoGenerate.sql",this)

  val insertSql:String =loadString.getString("/sql/dashu/dealRecordInfo/insertSql.sql",this)

}
