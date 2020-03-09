package dashu.constant_full
import util.sparkTool

object dealRecordInfo {
  //近6个月最大连续未开票间隔天数（销项）
  val nonDealDaysL6:String =sparkTool.loader.getString("/sql/dashu/dealRecordInfo/nonDealDaysL6.sql")

  //近12个月最大连续未开票间隔天数（销项）
  val nonDealDaysL12:String =sparkTool.loader.getString("/sql/dashu/dealRecordInfo/nonDealDaysL12.sql")

  //近24个月最大连续未开票间隔天数（销项）
  val nonDealDaysL24:String =sparkTool.loader.getString("/sql/dashu/dealRecordInfo/nonDealDaysL24.sql")

  //易记录信息目标表前半部分
  val dealRecordInfoPrehalf:String =sparkTool.loader.getString("/sql/dashu/dealRecordInfo/dealRecordInfoPrehalf.sql")

  val dealRecordInfoGenerate:String =sparkTool.loader.getString("/sql/dashu/dealRecordInfo/dealRecordInfoGenerate.sql")

  val insertSql:String =sparkTool.loader.getString("/sql/dashu/dealRecordInfo/insertSql.sql")

}
