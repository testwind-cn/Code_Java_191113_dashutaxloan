select
    t1.sellertaxno,
    dd.year
from (select distinct year from ${hivevar:DATABASE_DEST}.dim_month d
      where to_date(from_unixtime(unix_timestamp(d.month,'yyyyMM')))>=
            to_date(date_format(concat(year(add_months(date_sub(add_months(current_date,-1),dayofmonth(add_months(current_date,-1))-1),-12)),'-01','-01'),'yyyy-MM-dd'))
        and d.year <= year(add_months(current_date,-1))) dd cross join
     (
         select
             distinct sellertaxno
         from ${hivevar:DATABASE_DEST}.sale_region_list_tmp1
     ) t1
order by sellertaxno,year