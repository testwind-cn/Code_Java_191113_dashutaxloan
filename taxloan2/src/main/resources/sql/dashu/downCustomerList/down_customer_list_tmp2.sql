select
    t1.sellertaxno, --商户申请编号
    concat(substr(t1.invoicedate,1,4),0,floor(substr(t1.invoicedate,6,2)/3.1)+1) as quarter, --季度数
    t1.buyertaxno,
    SUM(CASE WHEN t1.cancelflag='0' THEN t1.totalamount + t1.totaltax ELSE 0 END) AS buyer_saleAmount, --交易对手发票合计金额税额总和（不含作废及红冲发票）  --计算每个交易对手的交易金额（万元）
    count(*) as buyer_dealNum --计算每个交易对手的交易次数
from ${hivevar:DATABASE_DEST}.saleinvoice_tmp t1
where t1.buyertaxno<>'' and t1.buyertaxno is not null and t1.buyertaxno not like '00%'  --剔除无效数据
  and from_unixtime(unix_timestamp(t1.invoicedate,'yyyy-MM-dd'))>= from_unixtime(unix_timestamp('1970-01-01','yyyy-MM-dd')) --日期大于等于1970-01-01
  and datediff(to_date(from_unixtime(unix_timestamp(t1.invoicedate,'yyyy-MM'))),
               to_date(from_unixtime(unix_timestamp( substr(current_date(),1,7),'yyyy-MM'))))<0  --剔除当月数据
  and to_date(from_unixtime(unix_timestamp(invoicedate,'yyyy-MM-dd')))>=  --取近两年的数据
      add_months(date_sub(current_date,dayofmonth(current_date)-1),-29) --29个月前的第一天
group by  t1.sellertaxno,concat(substr(t1.invoicedate,1,4),0,floor(substr(t1.invoicedate,6,2)/3.1)+1),buyertaxno
order by sellertaxno,quarter,buyertaxno
