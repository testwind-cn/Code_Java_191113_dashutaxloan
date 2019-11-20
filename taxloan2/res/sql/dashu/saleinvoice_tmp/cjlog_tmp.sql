with
cjlog_in as
(
    select
        CASE WHEN lower(taxno)='null'
            THEN ''
            ELSE COALESCE(taxno,'')
        END as taxno,
        CASE WHEN locate('.', oldtaxno) > 0
            THEN SUBSTRING(oldtaxno,1,locate('.', oldtaxno)-1)
            ELSE
                CASE WHEN lower(oldtaxno)='null'
                    THEN ''
                    ELSE COALESCE(oldtaxno,'')
                END
        END as oldtaxno
        ,mflag
        ,from_unixtime(unix_timestamp(finish_time,'yyyy-MM-dd HH:mm:ss')) finish_time
    from ${hivevar:DATABASE_SRC}.cjlog
    where
        finish_time is not null
        and finish_time!='null'
        and length(finish_time)>0
        and mflag=1
),
new_taxno_t as
(
    select
        taxno,
        oldtaxno
         , max(finish_time) last_time
         , min(finish_time) first_time
    from cjlog_in
    where
        mflag = 1 and finish_time is not null and
        (
            finish_time >= ${hivevar:LAST_TIME}
            ${hivevar:ADD_TAXNO}
        )
    group by
        taxno, oldtaxno
),
all_taxno_t as
(
    select
        taxno,
        oldtaxno
         , max(finish_time) last_time
         , min(finish_time) first_time
    from cjlog_in
    where
        mflag = 1 and finish_time is not null
    group by
        taxno, oldtaxno
)
select
    distinct
    coalesce( all_taxno_t.taxno,new_taxno_t.taxno) as taxno,
    coalesce( all_taxno_t.oldtaxno,new_taxno_t.oldtaxno) as oldtaxno
--     CASE WHEN  coalesce( all_taxno_t.last_time,new_taxno_t.last_time) > new_taxno_t.last_time
--         THEN coalesce( all_taxno_t.last_time,new_taxno_t.last_time)
--         ELSE new_taxno_t.last_time
--     END as last_time
from
    new_taxno_t
left join
    all_taxno_t
on new_taxno_t.taxno = all_taxno_t.oldtaxno
