select prehalf.sellertaxno            as taxno,
       nvl(prehalf.nonDealDays, 0)    as nondeal_days,
       nvl(prehalf.minInvoiceDate, 0) as invoice_date_min,
       nvl(prehalf.maxInvoiceDate, 0) as invoice_date_max,
       nvl(l6.nonDealDaysL6, 0)       as nondeal_days_l6,
       nvl(l12.nonDealDaysL12, 0)     as nondeal_days_l12,
       nvl(l24.nonDealDaysL24, 0)     as nondeal_days_l24,
       current_user()                 as create_user,
       current_date()                 as modify_time,
       current_user()                 as modify_user,
       current_date()                 as create_time
from ${hivevar:DATABASE_DEST}.deal_record_info_prehalf prehalf
         left join ${hivevar:DATABASE_DEST}.non_deal_days_l6 l6 on prehalf.sellertaxno = l6.sellertaxno
         left join ${hivevar:DATABASE_DEST}.non_deal_days_l12 l12 on prehalf.sellertaxno = l12.sellertaxno
         left join ${hivevar:DATABASE_DEST}.non_deal_days_l24 l24 on prehalf.sellertaxno = l24.sellertaxno
