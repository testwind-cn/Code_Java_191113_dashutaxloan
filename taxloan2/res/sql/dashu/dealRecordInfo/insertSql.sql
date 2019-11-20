INSERT INTO TABLE ${hivevar:DATABASE_DEST}.deal_record
    PARTITION(create_time)
SELECT
    taxno,
    nondeal_days,
    invoice_date_min,
    invoice_date_max,
    nondeal_days_l6,
    nondeal_days_l12,
    nondeal_days_l24,
    create_user,
    modify_time,
    modify_user,
    create_time
FROM tmp_deal_record
