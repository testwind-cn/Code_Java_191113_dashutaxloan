select
    sellertaxno,
    quarter,
    sum(buyer_saleAmount) as top5_buyer_saleAmount,
    sum(buyer_dealNum) as top5_buyer_dealNum
from (select
          sellertaxno,
          quarter,
          buyertaxno,
          buyer_saleAmount,
          row_number() over(partition by sellertaxno,quarter order by buyer_saleAmount desc ) as rn,
          buyer_dealNum
      from ${hivevar:DATABASE_DEST}.down_customer_list_tmp2) t where rn<=5 group by  sellertaxno,quarter
