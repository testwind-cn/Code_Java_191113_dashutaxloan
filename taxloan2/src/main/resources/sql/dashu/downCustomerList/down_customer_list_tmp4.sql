select
    tmp1.sellertaxno,
    tmp1.quarter,
    tmp1.CustomerCount,
    tmp3.top5_buyer_saleAmount as saleAmount,   --主要客户（前5）销售额（万元）
    round(100*tmp3.top5_buyer_saleAmount/tmp1.saleAmount,2) as saleAmountRatio, --前5大客户销售额占比(%)
    tmp3.top5_buyer_dealNum as dealNum,  --前5大客户交易次数（次）
    round(100*tmp3.top5_buyer_dealNum/tmp1.dealNum,2) as dealNumRatio  --前5大客户交易次数占比(%)
from ${hivevar:DATABASE_DEST}.down_customer_list_tmp1 tmp1
         left join ${hivevar:DATABASE_DEST}.down_customer_list_tmp3 tmp3
                   on tmp1.sellertaxno=tmp3.sellertaxno and tmp1.quarter=tmp3.quarter
