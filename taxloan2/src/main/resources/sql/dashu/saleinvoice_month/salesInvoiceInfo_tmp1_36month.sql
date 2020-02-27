-- salesInvoiceInfo_tmp1_36month 筛选近36个月的数据
select
    t1.sellertaxno, --商户号
    (case when  length (CAST(MONTH (t1.invoicedate) AS STRING)) =1 then concat(YEAR (t1.invoicedate),"0", MONTH (t1.invoicedate))
          else concat(YEAR (t1.invoicedate),MONTH (t1.invoicedate)) end) AS data_month, --所属时期 月度数
    sum(CASE WHEN cancelflag='0' THEN totalamount+totaltax ELSE 0 END) AS taxsale_amount,  --应税销售额 --红冲不剔除
    sum(totalamount+totaltax) AS total_amount,  -- 全部_含税金额合计
    sum(CASE WHEN invoicetype='s' AND cancelflag='0' THEN totalamount+totaltax ELSE 0 END) AS taxsale_amount_zp,  --销项增值税专用发票金额 --红冲不剔除
    sum(CASE WHEN invoicetype='c' AND cancelflag='0' THEN totalamount+totaltax ELSE 0 END) AS taxsale_amount_pp,  --销项增值税普通发票金额 --红冲不剔除
    abs(sum(CASE WHEN cancelflag='0' and (totalamount<0 OR totaltax<0) THEN totalamount+totaltax ELSE 0 END)) AS red_amount,  --红票金额  --作废标志为否
    sum(CASE WHEN cancelflag='1' THEN totalamount+totaltax ELSE 0 END) AS invalid_amount, --废票金额
    COUNT(*) AS valid_num,  --开票数量
    sum(CASE WHEN invoicetype='s' THEN 1 ELSE 0	END) AS valid_num_zp, --销项增值税专用发票数量
    sum(CASE WHEN invoicetype='c' THEN 1 ELSE 0	END) AS valid_num_pp,  --销项增值税普通发票数量
    sum(CASE WHEN cancelflag='0' and (totalamount<0 OR totaltax<0) THEN 1 ELSE 0 END) AS red_num,  --红票数量  --作废标志为否
    sum(CASE WHEN cancelflag='1' and totalamount+totaltax<>0 THEN 1 ELSE 0 END) AS invalid_num,  --废票数量  --含税金额不等于0
    sum(CASE WHEN cancelflag='0' THEN totaltax ELSE 0 END) AS tax_amount --交税金额
from
    ${hivevar:DATABASE_DEST}.saleinvoice_tmp t1
where t1.buyertaxno<>'' and t1.buyertaxno is not null and t1.buyertaxno not like '00%'  --剔除无效数据
  and from_unixtime(unix_timestamp(t1.invoicedate,'yyyy-MM-dd'))>= from_unixtime(unix_timestamp('1970-01-01','yyyy-MM-dd')) --日期大于等于1970-01-01
  and datediff(to_date(from_unixtime(unix_timestamp(t1.invoicedate,'yyyy-MM'))),
               to_date(from_unixtime(unix_timestamp( substr(current_date(),1,7),'yyyy-MM'))))<0  --剔除当月数据
  and to_date(from_unixtime(unix_timestamp(invoicedate,'yyyy-MM-dd')))>=  --取近两年的数据
      add_months(date_sub(current_date,dayofmonth(current_date)-1),-36) --36个月前的第一天
group by  t1.sellertaxno,(case when  length (CAST(MONTH (t1.invoicedate) AS STRING)) =1 then concat(YEAR (t1.invoicedate),"0", MONTH (t1.invoicedate))
                               else concat(YEAR (t1.invoicedate),MONTH (t1.invoicedate)) end)
order by t1.sellertaxno,data_month
