-- 关联36个月的维度表为计算同比、环比等指标
select
    dim.sellertaxno,
    dim.month,
    nvl(tmp1.taxsale_amount,0) as taxsale_amount
from ${hivevar:DATABASE_DEST}.dim_sellertaxno_36month dim
         left join salesInvoiceInfo_tmp1_36month tmp1
                   on dim.sellertaxno=tmp1.sellertaxno and dim.month=tmp1.data_month
order by sellertaxno,month
