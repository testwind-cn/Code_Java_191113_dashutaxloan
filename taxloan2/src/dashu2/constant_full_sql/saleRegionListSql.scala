package dashu2.constant_full_sql
import dashu2.constant_common

object saleRegionListSql {
  val sale_region_list_tmp1 =
    s"""
       |select
       |t1.sellertaxno, --商户申请编号
       |YEAR (t1.invoicedate) as year, --所属时期 年
       |mapping.provincename,   --省（自治区、直辖市）
       |count(distinct t1.buyertaxno ) as CustomerCount,  --计算全部交易方的客户数
       |SUM(CASE WHEN t1.cancelflag='0' THEN t1.totalamount + t1.totaltax ELSE 0 END) AS regionDealAmount, --交易对手发票合计金额税额总和（不含作废及红冲发票）  --交易金额（万元）
       |count(t1.buyertaxno) as regionDealNum --交易次数（次）
       |from
       |dm_taxloan.saleinvoice t1
       |left join dm_taxloan.idno_province_mapping mapping on
       |case when t1.buyertaxno like '91%' then substring(t1.buyertaxno,3,2) else substring(t1.buyertaxno,1,2) end = mapping.provinceid
       |where t1.buyertaxno<>'' and t1.buyertaxno is not null and t1.buyertaxno not like '00%'  --剔除无效数据
       |and from_unixtime(unix_timestamp(t1.invoicedate,'yyyy-MM-dd'))>= from_unixtime(unix_timestamp('1970-01-01','yyyy-MM-dd')) --日期大于等于1970-01-01
       |and datediff(to_date(from_unixtime(unix_timestamp(t1.invoicedate,'yyyy-MM'))),
       |		to_date(from_unixtime(unix_timestamp( substr(current_date(),1,7),'yyyy-MM'))))<0  --剔除当月数据
       |and to_date(from_unixtime(unix_timestamp(invoicedate,'yyyy-MM-dd')))>=
       |       to_date(date_format(concat(year(add_months(date_sub(add_months(current_date,-1),dayofmonth(add_months(current_date,-1))-1),-12)),'-01','-01'),'yyyy-MM-dd')) ---取上个月往上推一年的第一天
       |group by  t1.sellertaxno,YEAR (t1.invoicedate),mapping.provincename
       |order by t1.sellertaxno,year
     """.stripMargin

        val dim_sellertaxno_latest_2_years =
          s"""
             |select
             |	t1.sellertaxno,
             |	dd.year
             |from (select distinct year from dm_taxloan.dim_date d
             |where to_date(from_unixtime(unix_timestamp(d.month,'yyyyMM')))>=
             |to_date(date_format(concat(year(add_months(date_sub(add_months(current_date,-1),dayofmonth(add_months(current_date,-1))-1),-12)),'-01','-01'),'yyyy-MM-dd'))
             |and d.year <= year(add_months(current_date,-1))) dd cross join
             |(
             |select
             |	distinct sellertaxno
             |from dm_taxloan.sale_region_list_tmp1
             |) t1
             |order by sellertaxno,year
           """
    .stripMargin


  val sale_region_list_tmp2 =
    s"""
       |select
       |dim.sellertaxno,
       |dim.year,
       |tmp1.CustomerCount,
       |tmp1.provincename,
       |tmp1.regionDealAmount,
       |row_number() over(partition by dim.sellertaxno,dim.year order by tmp1.regionDealAmount desc) as rn,
       |tmp1.regionDealNum
       |from dm_taxloan.dim_sellertaxno_latest_2_years dim
       |left join dm_taxloan.sale_region_list_tmp1 tmp1 on dim.sellertaxno=tmp1.sellertaxno and dim.year=tmp1.year
     """.stripMargin

  val sale_region_list =
    s"""
       |select
       |sellertaxno as taxno,
       |year,
       |provincename customer_province,
       |nvl(regionDealAmount,0) as deal_amount_region,
       |rn as rank,
       |nvl(regionDealNum,0) as deal_num_region,
       |    current_user() as create_user,
       |    current_date() as modify_time,
       |    current_user() as modify_user,
       |    current_date() as create_time
       |from dm_taxloan.sale_region_list_tmp2 where rn<=5 order by taxno,year,rank
     """
     .stripMargin

  val insertSql =
    s"""
      |INSERT INTO TABLE ${constant_common.Const_common.DATABASE}.sale_region_list
      |PARTITION(create_time)
      |SELECT
      |taxno,
      |year,
      |rank,
      |customer_province,
      |deal_amount_region,
      |deal_num_region,
      |create_user,
      |modify_time,
      |modify_user,
      |create_time
      |FROM tmp_sale_region_list
    """.stripMargin

}
