-- 利用分析函数计算每个月前一个月和前一年的应税销售额
select
    sellertaxno,
    month,
    taxsale_amount,
    lag(taxsale_amount,1,0) over(partition by sellertaxno
        order by to_date(from_unixtime(unix_timestamp(month,'yyyyMM')))) as taxsale_amount_1_month_ago,
    lag(taxsale_amount,12,0) over(partition by sellertaxno
        order by to_date(from_unixtime(unix_timestamp(month,'yyyyMM')))) as taxsale_amount_1_year_ago
from ${hivevar:DATABASE_DEST}.salesInvoiceInfo_tmp1_36month_dim
