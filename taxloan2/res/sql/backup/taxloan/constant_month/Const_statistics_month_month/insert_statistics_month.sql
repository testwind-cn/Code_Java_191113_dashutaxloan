insert into ${Const_Common.DATABASE}.statistics_month
    partition(create_time)
select
    p1.mcht_cd,
    p1.month_pre1 as data_month,
    (case when p2.invoice_sc_cnt is null then 0 else p2.invoice_sc_cnt end) as invoice_sc_cnt,
    (case when p2.invoice_sc_cancel_cnt is null then 0 else p2.invoice_sc_cancel_cnt end) as invoice_sc_cancel_cnt,
    (case when p2.invoice_sc_red_cnt is null then 0 else p2.invoice_sc_red_cnt end) as invoice_sc_red_cnt,
    (case when p2.invoice_sc_total_sum is null then 0 else ROUND(p2.invoice_sc_total_sum,3) end) as invoice_sc_total_sum,
    (case when p2.invoice_cnt is null then 0 else p2.invoice_cnt end) as invoice_cnt,
    (case when p2.invoice_cr_cnt is null then 0 else p2.invoice_cr_cnt end) as invoice_cr_cnt,
    (case when p2.invoice_amt_sum is null then 0 else ROUND(p2.invoice_amt_sum,3) end) as invoice_amt_sum,
    (case when p2.invoice_tax_sum is null then 0 else ROUND(p2.invoice_tax_sum,3) end) as invoice_tax_sum,
    (case when p2.invoice_total_sum is null then 0 else ROUND(p2.invoice_total_sum,3) end) as invoice_total_sum,

    (case when p2.invoice_cancel_cnt is null then 0 else p2.invoice_cancel_cnt end) as invoice_cancel_cnt,
    (case when p2.invoice_red_cnt is null then 0 else p2.invoice_red_cnt end) as invoice_red_cnt,
    (case when p2.invoice_total_c_sum is null then 0 else ROUND(p2.invoice_total_c_sum,3) end) as invoice_total_c_sum,
    (case when p2.invoice_total_r_sum is null then 0 else ROUND(p2.invoice_total_r_sum,3) end) as invoice_total_r_sum,

    (case when p2.invoice_s_cnt is null then 0 else p2.invoice_s_cnt end) as invoice_s_cnt,
    (case when p2.invoice_s_cancel_cnt is null then 0 else p2.invoice_s_cancel_cnt end) as invoice_s_cancel_cnt,
    (case when p2.invoice_s_red_cnt is null then 0 else p2.invoice_s_red_cnt end) as invoice_s_red_cnt,
    (case when p2.invoice_s_amt_sum is null then 0 else ROUND(p2.invoice_s_amt_sum,3) end) as invoice_s_amt_sum,
    (case when p2.invoice_s_tax_sum is null then 0 else ROUND(p2.invoice_s_tax_sum,3) end) as invoice_s_tax_sum,
    (case when p2.invoice_s_total_sum is null then 0 else ROUND(p2.invoice_s_total_sum,3) end) as invoice_s_total_sum,
    (case when p2.invoice_c_cnt is null then 0 else p2.invoice_c_cnt end) as invoice_c_cnt,
    (case when p2.invoice_c_cancel_cnt is null then 0 else p2.invoice_c_cancel_cnt end) as invoice_c_cancel_cnt,
    (case when p2.invoice_c_red_cnt is null then 0 else p2.invoice_c_red_cnt end) as invoice_c_red_cnt,
    (case when p2.invoice_c_amt_sum is null then 0 else ROUND(p2.invoice_c_amt_sum,3) end) as invoice_c_amt_sum,
    (case when p2.invoice_c_tax_sum is null then 0 else ROUND(p2.invoice_c_tax_sum,3) end) as invoice_c_tax_sum,
    (case when p2.invoice_c_total_sum is null then 0 else ROUND(p2.invoice_c_total_sum,3) end) as invoice_c_total_sum,
    (case when p2.invoice_t_cnt is null then 0 else p2.invoice_t_cnt end) as invoice_t_cnt,
    (case when p2.invoice_t_cancel_cnt is null then 0 else p2.invoice_t_cancel_cnt end) as invoice_t_cancel_cnt,
    (case when p2.invoice_t_red_cnt is null then 0 else p2.invoice_t_red_cnt end) as invoice_t_red_cnt,
    (case when p2.invoice_t_amt_sum is null then 0 else ROUND(p2.invoice_t_amt_sum,3) end) as invoice_t_amt_sum,
    (case when p2.invoice_t_tax_sum is null then 0 else ROUND(p2.invoice_t_tax_sum,3) end) as invoice_t_tax_sum,
    (case when p2.invoice_t_total_sum is null then 0 else ROUND(p2.invoice_t_total_sum,3) end) as invoice_t_total_sum,
    (case when p2.invoice_p_cnt is null then 0 else p2.invoice_p_cnt end) as invoice_p_cnt,
    (case when p2.invoice_p_cancel_cnt is null then 0 else p2.invoice_p_cancel_cnt end) as invoice_p_cancel_cnt,
    (case when p2.invoice_p_red_cnt is null then 0 else p2.invoice_p_red_cnt end) as invoice_p_red_cnt,
    (case when p2.invoice_p_amt_sum is null then 0 else ROUND(p2.invoice_p_amt_sum,3) end) as invoice_p_amt_sum,
    (case when p2.invoice_p_tax_sum is null then 0 else ROUND(p2.invoice_p_tax_sum,3) end) as invoice_p_tax_sum,
    (case when p2.invoice_p_total_sum is null then 0 else ROUND(p2.invoice_p_total_sum,3) end) as invoice_p_total_sum,
    (case when p2.invoice_e_cnt is null then 0 else p2.invoice_e_cnt end) as invoice_e_cnt,
    (case when p2.invoice_e_cancel_cnt is null then 0 else p2.invoice_e_cancel_cnt end) as invoice_e_cancel_cnt,
    (case when p2.invoice_e_red_cnt is null then 0 else p2.invoice_e_red_cnt end) as invoice_e_red_cnt,
    (case when p2.invoice_e_amt_sum is null then 0 else ROUND(p2.invoice_e_amt_sum,3) end) as invoice_e_amt_sum,
    (case when p2.invoice_e_tax_sum is null then 0 else ROUND(p2.invoice_e_tax_sum,3) end) as invoice_e_tax_sum,
    (case when p2.invoice_e_total_sum is null then 0 else ROUND(p2.invoice_e_total_sum,3) end) as invoice_e_total_sum,
    (case when p2.invoice_r_cnt is null then 0 else p2.invoice_r_cnt end) as invoice_r_cnt,
    (case when p2.invoice_r_cancel_cnt is null then 0 else p2.invoice_r_cancel_cnt end) as invoice_r_cancel_cnt,
    (case when p2.invoice_r_red_cnt is null then 0 else p2.invoice_r_red_cnt end) as invoice_r_red_cnt,
    (case when p2.invoice_r_amt_sum is null then 0 else ROUND(p2.invoice_r_amt_sum,3) end) as invoice_r_amt_sum,
    (case when p2.invoice_r_tax_sum is null then 0 else ROUND(p2.invoice_r_tax_sum,3) end) as invoice_r_tax_sum,
    (case when p2.invoice_r_total_sum is null then 0 else ROUND(p2.invoice_r_total_sum,3) end) as invoice_r_total_sum,
    (case when p2.invoice_red_cnt_rate is null then 0.00 else ROUND(p2.invoice_red_cnt_rate,3) end) as invoice_red_cnt_rate,
    (case when p2.invoice_cancel_cnt_rate is null then 0.00 else ROUND(p2.invoice_cancel_cnt_rate,3) end) as invoice_cancel_cnt_rate,
    (case when p2.invoice_red_total_sum_rate is null then 0.00 else ROUND(p2.invoice_red_total_sum_rate,3) end) as invoice_red_total_sum_rate,
    (case when p2.invoice_cancel_total_sum_rate is null then 0.00 else ROUND(p2.invoice_cancel_total_sum_rate,3) end) as invoice_cancel_total_sum_rate,
    (case when p2.invoice_lt100_cnt is null then 0 else p2.invoice_lt100_cnt end) as invoice_lt100_cnt,
    (case when p2.invoice_lt100_cr_cnt is null then 0 else p2.invoice_lt100_cr_cnt end) as invoice_lt100_cr_cnt,
    (case when p2.invoice_lt100_total_sum is null then 0 else ROUND(p2.invoice_lt100_total_sum,3) end) as invoice_lt100_total_sum,
    (case when p2.invoice_lt1000_cnt is null then 0 else p2.invoice_lt1000_cnt end) as invoice_lt1000_cnt,
    (case when p2.invoice_lt1000_cr_cnt is null then 0 else p2.invoice_lt1000_cr_cnt end) as invoice_lt1000_cr_cnt,
    (case when p2.invoice_lt1000_total_sum is null then 0 else ROUND(p2.invoice_lt1000_total_sum,3) end) as invoice_lt1000_total_sum,
    (case when p2.invoice_lt2500_cnt is null then 0 else p2.invoice_lt2500_cnt end) as invoice_lt2500_cnt,
    (case when p2.invoice_lt2500_cr_cnt is null then 0 else p2.invoice_lt2500_cr_cnt end) as invoice_lt2500_cr_cnt,
    (case when p2.invoice_lt2500_total_sum is null then 0 else ROUND(p2.invoice_lt2500_total_sum,3) end) as invoice_lt2500_total_sum,
    (case when p2.invoice_lt5000_cnt is null then 0 else p2.invoice_lt5000_cnt end) as invoice_lt5000_cnt,
    (case when p2.invoice_lt5000_cr_cnt is null then 0 else p2.invoice_lt5000_cr_cnt end) as invoice_lt5000_cr_cnt,
    (case when p2.invoice_lt5000_total_sum is null then 0 else ROUND(p2.invoice_lt5000_total_sum,3) end) as invoice_lt5000_total_sum,
    (case when p2.invoice_lt10000_cnt is null then 0 else p2.invoice_lt10000_cnt end) as invoice_lt10000_cnt,
    (case when p2.invoice_lt10000_cr_cnt is null then 0 else p2.invoice_lt10000_cr_cnt end) as invoice_lt10000_cr_cnt,
    (case when p2.invoice_lt10000_total_sum is null then 0 else ROUND(p2.invoice_lt10000_total_sum,3) end) as invoice_lt10000_total_sum,
    (case when p2.invoice_gt10000_cnt is null then 0 else p2.invoice_gt10000_cnt end) as invoice_gt10000_cnt,
    (case when p2.invoice_gt10000_cr_cnt is null then 0 else p2.invoice_gt10000_cr_cnt end) as invoice_gt10000_cr_cnt,
    (case when p2.invoice_gt10000_total_sum is null then 0 else ROUND(p2.invoice_gt10000_total_sum,3) end) as invoice_gt10000_total_sum,
    (case when p2.invoice_detail_cnt is null then 0 else p2.invoice_detail_cnt end) as invoice_detail_cnt,
    (case when p2.invoice_detail_cr_cnt is null then 0 else p2.invoice_detail_cr_cnt end) as invoice_detail_cr_cnt,
    (case when p2.invoice_detail_total_sum is null then 0 else ROUND(p2.invoice_detail_total_sum,3) end) as invoice_detail_total_sum,
    (case when p2.buyer_cnt is null then 0 else p2.buyer_cnt end) as buyer_cnt,
    (case when p2.buyer_cnt_all is null then 0 else p2.buyer_cnt_all end) as buyer_cnt_all,
    (case when p2.buyer_a_cnt is null then 0 else p2.buyer_a_cnt end) as buyer_a_cnt,
    (case when p2.buyer_b_cnt is null then 0 else p2.buyer_b_cnt end) as buyer_b_cnt,
    (case when p2.buyer_c_cnt is null then 0 else p2.buyer_c_cnt end) as buyer_c_cnt,
    (case when p2.buyer_f_cnt is null then 0 else p2.buyer_f_cnt end) as buyer_f_cnt,
    '0' AS is_delete,

    current_user() AS create_user,
    from_unixtime(unix_timestamp()) as modify_time,
    current_user() AS modify_user,
    current_date() AS create_time

from
    statistics_month_tmp p1
        left join p2
                  on p1.mcht_cd=p2.mcht_cd and p1.month_pre1=p2.data_month