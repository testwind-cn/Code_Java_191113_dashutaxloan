select
    t1.sellertaxno,
    d.quarter
from ${hivevar:DATABASE_DEST}.dim_quarter d cross join
     (
         select
             distinct sellertaxno
         from ${hivevar:DATABASE_DEST}.sale_region_list_tmp1
     ) t1
order by sellertaxno,quarter
