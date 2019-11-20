package dashu2.constant_full_sql
import dashu2.constant_common

object dealRecordInfoSql {

  //近6个月最大连续未开票间隔天数（销项）
  val nonDealDaysL6 =
    s"""
       |with m as (
       |select sellertaxno,to_date(from_unixtime(unix_timestamp(invoicedate,'yyyy-MM-dd'))) as invoice_date   from ${constant_common.Const_common.DATABASE}.saleinvoice_tmp
       |       where to_date(from_unixtime(unix_timestamp(invoicedate,'yyyy-MM-dd')))>=
       |       add_months(date_sub(current_date(),dayofmonth(current_date())-1),-6) --6个月前的第一天
       |)
       |, tb as
       |(
       |select  ROW_NUMBER()over(partition by sellertaxno order by invoice_date) as rn
       |,*
       |from m
       |)
       |select sellertaxno,max(diff) as nonDealDaysL6 from (select a.sellertaxno,DATEDIFF(b.invoice_date,a.invoice_date) AS DIFF
       |from tb a
       |inner join tb b on b.rn = a.rn + 1 and a.sellertaxno = b.sellertaxno) a group by  sellertaxno
     """.stripMargin

  //近12个月最大连续未开票间隔天数（销项）
  val nonDealDaysL12 =
    s"""
       |with m as (
       |select sellertaxno,to_date(from_unixtime(unix_timestamp(invoicedate,'yyyy-MM-dd'))) as invoice_date   from ${constant_common.Const_common.DATABASE}.saleinvoice_tmp
       |       where to_date(from_unixtime(unix_timestamp(invoicedate,'yyyy-MM-dd')))>=
       |       add_months(date_sub(current_date(),dayofmonth(current_date())-1),-12) --12个月前的第一天
       |)
       |, tb as
       |(
       |select  ROW_NUMBER()over(partition by sellertaxno order by invoice_date) as rn
       |,*
       |from m
       |)
       |
 |select sellertaxno,max(diff) as nonDealDaysL12 from (select a.sellertaxno,DATEDIFF(b.invoice_date,a.invoice_date) AS DIFF
       |from tb a
       |inner join tb b on b.rn = a.rn + 1 and a.sellertaxno = b.sellertaxno) a group by  sellertaxno
       |
     """.stripMargin

  //近24个月最大连续未开票间隔天数（销项）
  val nonDealDaysL24 =
    s"""
       |with m as (
       |select sellertaxno,to_date(from_unixtime(unix_timestamp(invoicedate,'yyyy-MM-dd'))) as invoice_date   from ${constant_common.Const_common.DATABASE}.saleinvoice_tmp
       |       where to_date(from_unixtime(unix_timestamp(invoicedate,'yyyy-MM-dd')))>=
       |       add_months(date_sub(current_date(),dayofmonth(current_date())-1),-24) --24个月前的第一天
       |)
       |, tb as
       |(
       |select  ROW_NUMBER()over(partition by sellertaxno order by invoice_date) as rn
       |,*
       |from m
       |)
       |select sellertaxno,max(diff) as nonDealDaysL24 from (select a.sellertaxno,DATEDIFF(b.invoice_date,a.invoice_date) AS DIFF
       |from tb a
       |inner join tb b on b.rn = a.rn + 1 and a.sellertaxno = b.sellertaxno) a group by  sellertaxno
     """.stripMargin

  //易记录信息目标表前半部分
  val dealRecordInfoPrehalf =
    s"""
       |select
       |sellertaxno,
       |datediff(current_date(),to_date(from_unixtime(unix_timestamp(max(invoicedate),'yyyy-MM-dd')))) as nonDealDays,  --当前连续无交易记录天数
       |min(from_unixtime(unix_timestamp(invoicedate,'yyyy-MM-dd'))) as minInvoiceDate,  --最早一张发票时间(销项）
       |max(if(datediff(to_date(from_unixtime(unix_timestamp(invoicedate,'yyyy-MM-dd'))),current_date())=0,null,from_unixtime(unix_timestamp(invoicedate,'yyyy-MM-dd')))) as maxInvoiceDate--最近一张发票时间(销项）
       |from ${constant_common.Const_common.DATABASE}.saleinvoice_tmp group by sellertaxno
     """.stripMargin

  val dealRecordInfoGenerate =
    s"""
       |select
       |prehalf.sellertaxno as taxno,
       |nvl(prehalf.nonDealDays,0) as nondeal_days,
       |nvl(prehalf.minInvoiceDate,0) as invoice_date_min,
       |nvl(prehalf.maxInvoiceDate,0) as invoice_date_max,
       |nvl(l6.nonDealDaysL6,0) as nondeal_days_l6,
       |nvl(l12.nonDealDaysL12,0) as nondeal_days_l12,
       |nvl(l24.nonDealDaysL24,0) as nondeal_days_l24,
       |    current_user() as create_user,
       |    current_date() as modify_time,
       |    current_user() as modify_user,
       |    current_date() as create_time
       |from ${constant_common.Const_common.DATABASE}.deal_record_info_prehalf prehalf
       |left join ${constant_common.Const_common.DATABASE}.non_deal_days_l6 l6 on prehalf.sellertaxno = l6.sellertaxno
       |left join ${constant_common.Const_common.DATABASE}.non_deal_days_l12 l12 on prehalf.sellertaxno = l12.sellertaxno
       |left join ${constant_common.Const_common.DATABASE}.non_deal_days_l24 l24 on prehalf.sellertaxno = l24.sellertaxno
     """.stripMargin

  val insertSql=
    s"""
      |INSERT INTO TABLE ${constant_common.Const_common.DATABASE}.deal_record
      |PARTITION(create_time)
      |SELECT
      |taxno,
      |nondeal_days,
      |invoice_date_min,
      |invoice_date_max,
      |nondeal_days_l6,
      |nondeal_days_l12,
      |nondeal_days_l24,
      |create_user,
      |modify_time,
      |modify_user,
      |create_time
      |FROM tmp_deal_record
    """.stripMargin

}
