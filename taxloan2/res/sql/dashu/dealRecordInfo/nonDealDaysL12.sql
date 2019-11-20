-- 近12个月最大连续未开票间隔天数（销项）
with m as (
    select sellertaxno, to_date(from_unixtime(unix_timestamp(invoicedate, 'yyyy-MM-dd'))) as invoice_date
    from ${hivevar:DATABASE_DEST}.saleinvoice_tmp
    where to_date(from_unixtime(unix_timestamp(invoicedate, 'yyyy-MM-dd'))) >=
          add_months(date_sub(current_date(), dayofmonth(current_date()) - 1), -12) --12个月前的第一天
)
   , tb as
    (
        select ROW_NUMBER() over (partition by sellertaxno order by invoice_date) as rn
             , *
        from m
    )

select sellertaxno, max(diff) as nonDealDaysL12
from (select a.sellertaxno, DATEDIFF(b.invoice_date, a.invoice_date) AS DIFF
      from tb a
               inner join tb b on b.rn = a.rn + 1 and a.sellertaxno = b.sellertaxno) a
group by sellertaxno
