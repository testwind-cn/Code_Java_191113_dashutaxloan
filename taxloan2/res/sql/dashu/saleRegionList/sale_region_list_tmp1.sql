select
    t1.sellertaxno, --商户申请编号
    YEAR (t1.invoicedate) as year, --所属时期 年
    mapping.provincename,   --省（自治区、直辖市）
    count(distinct t1.buyertaxno ) as CustomerCount,  --计算全部交易方的客户数
    SUM(CASE WHEN t1.cancelflag='0' THEN t1.totalamount + t1.totaltax ELSE 0 END) AS regionDealAmount, --交易对手发票合计金额税额总和（不含作废及红冲发票）  --交易金额（万元）
    count(t1.buyertaxno) as regionDealNum --交易次数（次）
from
    ${hivevar:DATABASE_SRC}.saleinvoice t1
        left join dim_db.idno_province_mapping mapping on
            case when t1.buyertaxno like '91%' then substring(t1.buyertaxno,3,2) else substring(t1.buyertaxno,1,2) end = mapping.provinceid
where t1.buyertaxno<>'' and t1.buyertaxno is not null and t1.buyertaxno not like '00%'  --剔除无效数据
  and from_unixtime(unix_timestamp(t1.invoicedate,'yyyy-MM-dd'))>= from_unixtime(unix_timestamp('1970-01-01','yyyy-MM-dd')) --日期大于等于1970-01-01
  and datediff(to_date(from_unixtime(unix_timestamp(t1.invoicedate,'yyyy-MM'))),
               to_date(from_unixtime(unix_timestamp( substr(current_date(),1,7),'yyyy-MM'))))<0  --剔除当月数据
  and to_date(from_unixtime(unix_timestamp(invoicedate,'yyyy-MM-dd')))>=
      to_date(date_format(concat(year(add_months(date_sub(add_months(current_date,-1),dayofmonth(add_months(current_date,-1))-1),-12)),'-01','-01'),'yyyy-MM-dd')) ---取上个月往上推一年的第一天
group by  t1.sellertaxno,YEAR (t1.invoicedate),mapping.provincename
order by t1.sellertaxno,year
