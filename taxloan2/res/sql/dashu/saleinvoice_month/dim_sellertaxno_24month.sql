select
    t1.sellertaxno,
    d.month
from ${hivevar:DATABASE_DEST}.dim_date d cross join
     (
         select
             distinct sellertaxno
         from ${hivevar:DATABASE_DEST}.sale_region_list_tmp1
     ) t1
where datediff(to_date(from_unixtime(unix_timestamp(d.month,'yyyyMM'))),
               to_date(from_unixtime(unix_timestamp( substr(add_months(current_date(),-24),1,7),'yyyy-MM'))))>=0  --取近24个月的维度
  and
        datediff(to_date(from_unixtime(unix_timestamp(d.month,'yyyyMM'))),
                 to_date(from_unixtime(unix_timestamp( substr(current_date(),1,7),'yyyy-MM'))))< 0
order by sellertaxno,month