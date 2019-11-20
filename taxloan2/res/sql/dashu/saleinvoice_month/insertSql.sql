INSERT INTO TABLE ${hivevar:DATABASE_DEST}.saleinvoice_month
    PARTITION(create_time)
SELECT
    taxno,
    month,
    taxsale_amount,
    taxsale_amount_zp,
    taxsale_amount_pp,
    tax_amount,
    red_amount,
    invalid_amount,
    valid_num,
    valid_num_zp,
    valid_num_pp,
    red_num,
    invalid_num,
    red_amount_ratio,
    invalid_amount_ratio,
    red_num_ratio,
    invalid_num_ratio,
    taxsale_amount_yoy,
    taxsale_amount_mom,
    create_user,
    modify_time,
    modify_user,
    create_time
FROM tmp_saleinvoice_month
