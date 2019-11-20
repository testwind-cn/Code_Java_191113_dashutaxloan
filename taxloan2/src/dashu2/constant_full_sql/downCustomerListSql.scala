package dashu2.constant_full_sql
import dashu2.constant_common

object downCustomerListSql {

  val down_customer_list_tmp1=
    s"""
      |select
      |t1.sellertaxno, --商户申请编号
      |concat(substr(t1.invoicedate,1,4),0,floor(substr(t1.invoicedate,6,2)/3.1)+1) as quarter, --季度数
      |count(distinct t1.buyertaxno ) as CustomerCount,  --计算全部交易方的客户数
      |SUM(CASE WHEN t1.totalamount > 0.0 AND t1.totaltax>=0.0 AND t1.cancelflag='0' THEN t1.totalamount + t1.totaltax ELSE 0 END) AS saleAmount, --交易对手发票合计金额税额总和（不含作废及红冲发票）  --交易金额（万元）
      |count(t1.buyertaxno) as dealNum --交易次数（次）
      |from ${constant_common.Const_common.DATABASE}.saleinvoice_tmp t1
      |where t1.buyertaxno<>'' and t1.buyertaxno is not null and t1.buyertaxno not like '00%'  --剔除无效数据
      |and from_unixtime(unix_timestamp(t1.invoicedate,'yyyy-MM-dd'))>= from_unixtime(unix_timestamp('1970-01-01','yyyy-MM-dd')) --日期大于等于1970-01-01
      |and datediff(to_date(from_unixtime(unix_timestamp(t1.invoicedate,'yyyy-MM'))),
      |		to_date(from_unixtime(unix_timestamp( substr(current_date(),1,7),'yyyy-MM'))))<0  --剔除当月数据
      |and to_date(from_unixtime(unix_timestamp(invoicedate,'yyyy-MM-dd')))>=  --取近两年的数据
      |       add_months(date_sub(current_date,dayofmonth(current_date)-1),-29) --29个月前的第一天
      |group by  t1.sellertaxno,concat(substr(t1.invoicedate,1,4),0,floor(substr(t1.invoicedate,6,2)/3.1)+1)
      |order by t1.sellertaxno,quarter
    """.stripMargin

  val down_customer_list_tmp2=
    s"""
      |select
      |t1.sellertaxno, --商户申请编号
      |concat(substr(t1.invoicedate,1,4),0,floor(substr(t1.invoicedate,6,2)/3.1)+1) as quarter, --季度数
      |t1.buyertaxno,
      |SUM(CASE WHEN t1.cancelflag='0' THEN t1.totalamount + t1.totaltax ELSE 0 END) AS buyer_saleAmount, --交易对手发票合计金额税额总和（不含作废及红冲发票）  --计算每个交易对手的交易金额（万元）
      |count(*) as buyer_dealNum --计算每个交易对手的交易次数
      |from ${constant_common.Const_common.DATABASE}.saleinvoice_tmp t1
      |where t1.buyertaxno<>'' and t1.buyertaxno is not null and t1.buyertaxno not like '00%'  --剔除无效数据
      |and from_unixtime(unix_timestamp(t1.invoicedate,'yyyy-MM-dd'))>= from_unixtime(unix_timestamp('1970-01-01','yyyy-MM-dd')) --日期大于等于1970-01-01
      |and datediff(to_date(from_unixtime(unix_timestamp(t1.invoicedate,'yyyy-MM'))),
      |		to_date(from_unixtime(unix_timestamp( substr(current_date(),1,7),'yyyy-MM'))))<0  --剔除当月数据
      |and to_date(from_unixtime(unix_timestamp(invoicedate,'yyyy-MM-dd')))>=  --取近两年的数据
      |       add_months(date_sub(current_date,dayofmonth(current_date)-1),-29) --29个月前的第一天
      |group by  t1.sellertaxno,concat(substr(t1.invoicedate,1,4),0,floor(substr(t1.invoicedate,6,2)/3.1)+1),buyertaxno
      |order by sellertaxno,quarter,buyertaxno
    """.stripMargin

  val down_customer_list_tmp3=
    s"""
      |select
      |sellertaxno,
      |quarter,
      |sum(buyer_saleAmount) as top5_buyer_saleAmount,
      |sum(buyer_dealNum) as top5_buyer_dealNum
      |from (select
      |sellertaxno,
      |quarter,
      |buyertaxno,
      |buyer_saleAmount,
      |row_number() over(partition by sellertaxno,quarter order by buyer_saleAmount desc ) as rn,
      |buyer_dealNum
      |from ${constant_common.Const_common.DATABASE}.down_customer_list_tmp2) t where rn<=5 group by  sellertaxno,quarter
    """.stripMargin

  val down_customer_list_tmp4=
    s"""
      |select
      |tmp1.sellertaxno,
      |tmp1.quarter,
      |tmp1.CustomerCount,
      |tmp3.top5_buyer_saleAmount as saleAmount,   --主要客户（前5）销售额（万元）
      |round(100*tmp3.top5_buyer_saleAmount/tmp1.saleAmount,2) as saleAmountRatio, --前5大客户销售额占比(%)
      |tmp3.top5_buyer_dealNum as dealNum,  --前5大客户交易次数（次）
      |round(100*tmp3.top5_buyer_dealNum/tmp1.dealNum,2) as dealNumRatio  --前5大客户交易次数占比(%)
      |from ${constant_common.Const_common.DATABASE}.down_customer_list_tmp1 tmp1
      |left join ${constant_common.Const_common.DATABASE}.down_customer_list_tmp3 tmp3
      |on tmp1.sellertaxno=tmp3.sellertaxno and tmp1.quarter=tmp3.quarter
    """.stripMargin

  val dim_quarter=
    s"""
      |select distinct concat(year,0,quarter) as quarter from allinpal_dw.dim_date d
      |where datediff(to_date(from_unixtime(unix_timestamp(substr(d.date,1,7),'yyyy-MM'))),
      |		to_date(from_unixtime(unix_timestamp( substr(add_months(current_date(),-24),1,7),'yyyy-MM'))))>=0
      |and
      |datediff(to_date(from_unixtime(unix_timestamp(substr(d.date,1,7),'yyyy-MM'))),
      |to_date(from_unixtime(unix_timestamp( substr(current_date(),1,7),'yyyy-MM'))))< 0 --取近24个月的维度
    """.stripMargin

  val dim_sellertaxno_quarter_24month=
    s"""
      |select
      |	t1.sellertaxno,
      |	d.quarter
      |from ${constant_common.Const_common.DATABASE}.dim_quarter d cross join
      |(
      |select
      |	distinct sellertaxno
      |from ${constant_common.Const_common.DATABASE}.sale_region_list_tmp1
      |) t1
      |order by sellertaxno,quarter
    """.stripMargin

  val down_customer_list=
    s"""
      |select
      |dim.sellertaxno as taxno,
      |dim.quarter as quarter,
      |nvl(tmp4.CustomerCount,0) as customer_num,
      |nvl(tmp4.saleAmount,0) as top5sale_amount,
      |nvl(tmp4.saleAmountRatio,0) as top5sale_amount_ratio,
      |nvl(tmp4.dealNum,0) as top5deal_num,
      |nvl(tmp4.dealNumRatio,0) as top5deal_num_ratio,
      |    current_user() as create_user,
      |    current_date() as modify_time,
      |    current_user() as modify_user,
      |    current_date() as create_time
      |from ${constant_common.Const_common.DATABASE}.dim_sellertaxno_quarter_24month dim
      |left join ${constant_common.Const_common.DATABASE}.down_customer_list_tmp4 tmp4
      |on dim.sellertaxno=tmp4.sellertaxno and dim.quarter=tmp4.quarter order by taxno,quarter
    """.stripMargin

  val insertSql =
    s"""
      |INSERT INTO TABLE ${constant_common.Const_common.DATABASE}.down_customer_list
      |PARTITION(create_time)
      |SELECT
      |taxno,
      |quarter,
      |customer_num,
      |top5sale_amount,
      |top5sale_amount_ratio,
      |top5deal_num,
      |top5deal_num_ratio,
      |create_user,
      |modify_time,
      |modify_user,
      |create_time
      |FROM tmp_down_customer_list
    """.stripMargin
}
