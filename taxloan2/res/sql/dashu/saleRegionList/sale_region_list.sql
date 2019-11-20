select
    sellertaxno as taxno,
    year,
    provincename customer_province,
    nvl(regionDealAmount,0) as deal_amount_region,
    rn as rank,
    nvl(regionDealNum,0) as deal_num_region,
    current_user() as create_user,
    current_date() as modify_time,
    current_user() as modify_user,
    current_date() as create_time
from ${hivevar:DATABASE_DEST}.sale_region_list_tmp2 where rn<=5 order by taxno,year,rank
