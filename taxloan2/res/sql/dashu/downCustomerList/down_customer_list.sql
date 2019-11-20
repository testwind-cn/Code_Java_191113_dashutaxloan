select
    dim.sellertaxno as taxno,
    dim.quarter as quarter,
    nvl(tmp4.CustomerCount,0) as customer_num,
    nvl(tmp4.saleAmount,0) as top5sale_amount,
    nvl(tmp4.saleAmountRatio,0) as top5sale_amount_ratio,
    nvl(tmp4.dealNum,0) as top5deal_num,
    nvl(tmp4.dealNumRatio,0) as top5deal_num_ratio,
    current_user() as create_user,
    current_date() as modify_time,
    current_user() as modify_user,
    current_date() as create_time
from ${hivevar:DATABASE_DEST}.dim_sellertaxno_quarter_24month dim
         left join ${hivevar:DATABASE_DEST}.down_customer_list_tmp4 tmp4
                   on dim.sellertaxno=tmp4.sellertaxno and dim.quarter=tmp4.quarter order by taxno,quarter
