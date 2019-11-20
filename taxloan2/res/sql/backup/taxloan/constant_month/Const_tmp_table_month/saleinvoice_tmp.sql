select
    sellertaxno,sellername,selleraddtel,invoicedate,data_month,sellerbankno,
    oldtaxno,invoiceid,buyername,buyertaxno,totalamount,totaltax,cancelflag,invoicetype,jztype
from ${Const_Common.DATABASE}.month_pre1 m
         left join
     (
         select sellertaxno,sellername,selleraddtel,sellerbankno,invoicedate,data_month,
                oldtaxno,invoiceid,buyername,buyertaxno,totalamount,totaltax,cancelflag,invoicetype,jztype,
                row_number() over(partition by invoiceid order by invoicedate) as rank
         from ${Const_Common.DATABASE}.saleinvoice_tmp3
     )a
     on m.month_pre1=a.data_month
where a.rank=1 and a.invoiceid!='NULL'