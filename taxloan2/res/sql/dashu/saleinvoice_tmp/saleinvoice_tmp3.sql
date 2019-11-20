select * from ${hivevar:DATABASE_DEST}.saleinvoice_tmp1
union all
select * from ${hivevar:DATABASE_DEST}.saleinvoice_tmp2
