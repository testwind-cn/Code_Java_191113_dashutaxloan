select
    dim.sellertaxno,
    dim.year,
    tmp1.CustomerCount,
    tmp1.provincename,
    tmp1.regionDealAmount,
    row_number() over(partition by dim.sellertaxno,dim.year order by tmp1.regionDealAmount desc) as rn,
    tmp1.regionDealNum
from ${hivevar:DATABASE_DEST}.dim_sellertaxno_latest_2_years dim
         left join ${hivevar:DATABASE_DEST}.sale_region_list_tmp1 tmp1 on dim.sellertaxno=tmp1.sellertaxno and dim.year=tmp1.year
