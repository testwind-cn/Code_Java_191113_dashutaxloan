-- 生成sellertaxno_24month商户最近36month维度表
select
    t1.sellertaxno,
    d.month_value as month
from dim_db.dim_month d cross join
     (
         select
             distinct sellertaxno
         from ${hivevar:DATABASE_DEST}.salesInvoiceInfo_tmp1_24month
     ) t1
where datediff(to_date(from_unixtime(unix_timestamp(d.month_value,'yyyyMM'))),
               to_date(from_unixtime(unix_timestamp( substr(add_months(current_date(),-36),1,7),'yyyy-MM'))))>=0
  and
        datediff(to_date(from_unixtime(unix_timestamp(d.month_value,'yyyyMM'))),
                 to_date(from_unixtime(unix_timestamp( substr(current_date(),1,7),'yyyy-MM'))))< 0 --取近36个月的维度
order by sellertaxno,month
