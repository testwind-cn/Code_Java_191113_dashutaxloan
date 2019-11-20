-- 易记录信息目标表前半部分
select sellertaxno,
       datediff(current_date(), to_date(from_unixtime(unix_timestamp(max(invoicedate), 'yyyy-MM-dd'))))                                                                         as nonDealDays,    --当前连续无交易记录天数
       min(from_unixtime(unix_timestamp(invoicedate, 'yyyy-MM-dd')))                                                                                                            as minInvoiceDate, --最早一张发票时间(销项）
       max(if(datediff(to_date(from_unixtime(unix_timestamp(invoicedate, 'yyyy-MM-dd'))), current_date()) = 0, null, from_unixtime(unix_timestamp(invoicedate, 'yyyy-MM-dd')))) as maxInvoiceDate--最近一张发票时间(销项）
from ${hivevar:DATABASE_DEST}.saleinvoice_tmp
group by sellertaxno
