INSERT INTO TABLE ${hivevar:DATABASE_DEST}.down_customer_list
    PARTITION(create_time)
SELECT
    taxno,
    quarter,
    customer_num,
    top5sale_amount,
    top5sale_amount_ratio,
    top5deal_num,
    top5deal_num_ratio,
    create_user,
    modify_time,
    modify_user,
    create_time
FROM tmp_down_customer_list
