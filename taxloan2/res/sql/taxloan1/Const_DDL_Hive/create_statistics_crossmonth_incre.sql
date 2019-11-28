
CREATE TABLE  IF NOT EXISTS ${hivevar:DATABASE_DEST}.statistics_crossmonth_incre(
  `mcht_cd` string,
  `data_month` string,
  `invoice_sc_cnt_l3m` int,
  `invoice_sc_cancel_cnt_l3m` int,
  `invoice_sc_red_cnt_l3m` int,
  `invoice_sc_total_sum_l3m` decimal(20,2),
  `invoice_cnt_l3m` int,
  `invoice_cr_cnt_l3m` int,
  `invoice_total_sum_l3m` decimal(20,2),
  `invoice_s_cnt_l3m` int,
  `invoice_s_cancel_cnt_l3m` int,
  `invoice_s_red_cnt_l3m` int,
  `invoice_s_total_sum_l3m` decimal(20,2),
  `invoice_c_cnt_l3m` int,
  `invoice_c_cancel_cnt_l3m` int,
  `invoice_c_red_cnt_l3m` int,
  `invoice_c_total_sum_l3m` decimal(20,2),
  `invoice_t_cnt_l3m` int,
  `invoice_t_cancel_cnt_l3m` int,
  `invoice_t_red_cnt_l3m` int,
  `invoice_t_total_sum_l3m` decimal(20,2),
  `invoice_p_cnt_l3m` int,
  `invoice_p_cancel_cnt_l3m` int,
  `invoice_p_red_cnt_l3m` int,
  `invoice_p_total_sum_l3m` decimal(20,2),
  `invoice_e_cnt_l3m` int,
  `invoice_e_cancel_cnt_l3m` int,
  `invoice_e_red_cnt_l3m` int,
  `invoice_e_total_sum_l3m` decimal(20,2),
  `invoice_r_cnt_l3m` int,
  `invoice_r_cancel_cnt_l3m` int,
  `invoice_r_red_cnt_l3m` int,
  `invoice_r_total_sum_l3m` decimal(20,2),
  `invoice_red_cnt_rate_l3m` decimal(20,2),
  `invoice_cancel_cnt_rate_l3m` decimal(20,2),
  `invoice_red_total_sum_rate_l3m` decimal(20,2),
  `invoice_cancel_total_sum_rate_l3m` decimal(20,2),
  `invoice_lt100_cnt_l3m` int,
  `invoice_lt100_cr_cnt_l3m` int,
  `invoice_lt100_total_sum_l3m` decimal(20,2),
  `invoice_lt1000_cnt_l3m` int,
  `invoice_lt1000_cr_cnt_l3m` int,
  `invoice_lt1000_total_sum_l3m` decimal(20,2),
  `invoice_lt2500_cnt_l3m` int,
  `invoice_lt2500_cr_cnt_l3m` int,
  `invoice_lt2500_total_sum_l3m` decimal(20,2),
  `invoice_lt5000_cnt_l3m` int,
  `invoice_lt5000_cr_cnt_l3m` int,
  `invoice_lt5000_total_sum_l3m` decimal(20,2),
  `invoice_lt10000_cnt_l3m` int,
  `invoice_lt10000_cr_cnt_l3m` int,
  `invoice_lt10000_total_sum_l3m` decimal(20,2),
  `invoice_gt10000_cnt_l3m` int,
  `invoice_gt10000_cr_cnt_l3m` int,
  `invoice_gt10000_total_sum_l3m` decimal(20,2),
  `invoice_detail_cnt_l3m` int,
  `invoice_detail_cr_cnt_l3m` int,
  `invoice_detail_total_sum_l3m` decimal(20,2),
  `buyer_cnt_l3m` int,
  `invoice_sc_cnt_l6m` int,
  `invoice_sc_cancel_cnt_l6m` int,
  `invoice_sc_red_cnt_l6m` int,
  `invoice_sc_total_sum_l6m` decimal(20,2),
  `invoice_cnt_l6m` int,
  `invoice_cr_cnt_l6m` int,
  `invoice_total_sum_l6m` decimal(20,2),
  `invoice_s_cnt_l6m` int,
  `invoice_s_cancel_cnt_l6m` int,
  `invoice_s_red_cnt_l6m` int,
  `invoice_s_total_sum_l6m` decimal(20,2),
  `invoice_c_cnt_l6m` int,
  `invoice_c_cancel_cnt_l6m` int,
  `invoice_c_red_cnt_l6m` int,
  `invoice_c_total_sum_l6m` decimal(20,2),
  `invoice_t_cnt_l6m` int,
  `invoice_t_cancel_cnt_l6m` int,
  `invoice_t_red_cnt_l6m` int,
  `invoice_t_total_sum_l6m` decimal(20,2),
  `invoice_p_cnt_l6m` int,
  `invoice_p_cancel_cnt_l6m` int,
  `invoice_p_red_cnt_l6m` int,
  `invoice_p_total_sum_l6m` decimal(20,2),
  `invoice_e_cnt_l6m` int,
  `invoice_e_cancel_cnt_l6m` int,
  `invoice_e_red_cnt_l6m` int,
  `invoice_e_total_sum_l6m` decimal(20,2),
  `invoice_r_cnt_l6m` int,
  `invoice_r_cancel_cnt_l6m` int,
  `invoice_r_red_cnt_l6m` int,
  `invoice_r_total_sum_l6m` decimal(20,2),
  `invoice_red_cnt_rate_l6m` decimal(20,2),
  `invoice_cancel_cnt_rate_l6m` decimal(20,2),
  `invoice_red_total_sum_rate_l6m` decimal(20,2),
  `invoice_cancel_total_sum_rate_l6m` decimal(20,2),
  `invoice_lt100_cnt_l6m` int,
  `invoice_lt100_cr_cnt_l6m` int,
  `invoice_lt100_total_sum_l6m` decimal(20,2),
  `invoice_lt1000_cnt_l6m` int,
  `invoice_lt1000_cr_cnt_l6m` int,
  `invoice_lt1000_total_sum_l6m` decimal(20,2),
  `invoice_lt2500_cnt_l6m` int,
  `invoice_lt2500_cr_cnt_l6m` int,
  `invoice_lt2500_total_sum_l6m` decimal(20,2),
  `invoice_lt5000_cnt_l6m` int,
  `invoice_lt5000_cr_cnt_l6m` int,
  `invoice_lt5000_total_sum_l6m` decimal(20,2),
  `invoice_lt10000_cnt_l6m` int,
  `invoice_lt10000_cr_cnt_l6m` int,
  `invoice_lt10000_total_sum_l6m` decimal(20,2),
  `invoice_gt10000_cnt_l6m` int,
  `invoice_gt10000_cr_cnt_l6m` int,
  `invoice_gt10000_total_sum_l6m` decimal(20,2),
  `invoice_detail_cnt_l6m` int,
  `invoice_detail_cr_cnt_l6m` int,
  `invoice_detail_total_sum_l6m` decimal(20,2),
  `buyer_cnt_l6m` int,
  `buyer_a_invoice_cnt_l6m` int,
  `buyer_b_invoice_cnt_l6m` int,
  `buyer_c_invoice_cnt_l6m` int,
  `buyer_f_invoice_cnt_l6m` int,
  `buyer_a_amt_sum_l6m` decimal(20,2),
  `buyer_b_amt_sum_l6m` decimal(20,2),
  `buyer_c_amt_sum_l6m` decimal(20,2),
  `buyer_f_amt_sum_l6m` decimal(20,2),
  `invoice_sc_cnt_l12m` int,
  `invoice_sc_cancel_cnt_l12m` int,
  `invoice_sc_red_cnt_l12m` int,
  `invoice_sc_total_sum_l12m` decimal(20,2),
  `invoice_cnt_l12m` int,
  `invoice_cr_cnt_l12m` int,
  `invoice_total_sum_l12m` decimal(20,2),
  `invoice_s_cnt_l12m` int,
  `invoice_s_cancel_cnt_l12m` int,
  `invoice_s_red_cnt_l12m` int,
  `invoice_s_total_sum_l12m` decimal(20,2),
  `invoice_c_cnt_l12m` int,
  `invoice_c_cancel_cnt_l12m` int,
  `invoice_c_red_cnt_l12m` int,
  `invoice_c_total_sum_l12m` decimal(20,2),
  `invoice_t_cnt_l12m` int,
  `invoice_t_cancel_cnt_l12m` int,
  `invoice_t_red_cnt_l12m` int,
  `invoice_t_total_sum_l12m` decimal(20,2),
  `invoice_p_cnt_l12m` int,
  `invoice_p_cancel_cnt_l12m` int,
  `invoice_p_red_cnt_l12m` int,
  `invoice_p_total_sum_l12m` decimal(20,2),
  `invoice_e_cnt_l12m` int,
  `invoice_e_cancel_cnt_l12m` int,
  `invoice_e_red_cnt_l12m` int,
  `invoice_e_total_sum_l12m` decimal(20,2),
  `invoice_r_cnt_l12m` int,
  `invoice_r_cancel_cnt_l12m` int,
  `invoice_r_red_cnt_l12m` int,
  `invoice_r_total_sum_l12m` decimal(20,2),
  `invoice_red_cnt_rate_l12m` decimal(20,2),
  `invoice_cancel_cnt_rate_l12m` decimal(20,2),
  `invoice_red_total_sum_rate_l12m` decimal(20,2),
  `invoice_cancel_total_sum_rate_l12m` decimal(20,2),
  `invoice_lt100_cnt_l12m` int,
  `invoice_lt100_cr_cnt_l12m` int,
  `invoice_lt100_total_sum_l12m` decimal(20,2),
  `invoice_lt1000_cnt_l12m` int,
  `invoice_lt1000_cr_cnt_l12m` int,
  `invoice_lt1000_total_sum_l12m` decimal(20,2),
  `invoice_lt2500_cnt_l12m` int,
  `invoice_lt2500_cr_cnt_l12m` int,
  `invoice_lt2500_total_sum_l12m` decimal(20,2),
  `invoice_lt5000_cnt_l12m` int,
  `invoice_lt5000_cr_cnt_l12m` int,
  `invoice_lt5000_total_sum_l12m` decimal(20,2),
  `invoice_lt10000_cnt_l12m` int,
  `invoice_lt10000_cr_cnt_l12m` int,
  `invoice_lt10000_total_sum_l12m` decimal(20,2),
  `invoice_gt10000_cnt_l12m` int,
  `invoice_gt10000_cr_cnt_l12m` int,
  `invoice_gt10000_total_sum_l12m` decimal(20,2),
  `invoice_detail_cnt_l12m` int,
  `invoice_detail_cr_cnt_l12m` int,
  `invoice_detail_total_sum_l12m` decimal(20,2),
  `buyer_cnt_l12m` int,
  `buyer_a_invoice_cnt_l12m` int,
  `buyer_b_invoice_cnt_l12m` int,
  `buyer_c_invoice_cnt_l12m` int,
  `buyer_f_invoice_cnt_l12m` int,
  `buyer_a_amt_sum_l12m` decimal(20,2),
  `buyer_b_amt_sum_l12m` decimal(20,2),
  `buyer_c_amt_sum_l12m` decimal(20,2),
  `buyer_f_amt_sum_l12m` decimal(20,2),
  `invoice_sc_cnt_l24m` int,
  `invoice_sc_cancel_cnt_l24m` int,
  `invoice_sc_red_cnt_l24m` int,
  `invoice_sc_total_sum_l24m` decimal(20,2),
  `invoice_cnt_l24m` int,
  `invoice_cr_cnt_l24m` int,
  `invoice_total_sum_l24m` decimal(20,2),
  `invoice_s_cnt_l24m` int,
  `invoice_s_cancel_cnt_l24m` int,
  `invoice_s_red_cnt_l24m` int,
  `invoice_s_total_sum_l24m` decimal(20,2),
  `invoice_c_cnt_l24m` int,
  `invoice_c_cancel_cnt_l24m` int,
  `invoice_c_red_cnt_l24m` int,
  `invoice_c_total_sum_l24m` decimal(20,2),
  `invoice_t_cnt_l24m` int,
  `invoice_t_cancel_cnt_l24m` int,
  `invoice_t_red_cnt_l24m` int,
  `invoice_t_total_sum_l24m` decimal(20,2),
  `invoice_p_cnt_l24m` int,
  `invoice_p_cancel_cnt_l24m` int,
  `invoice_p_red_cnt_l24m` int,
  `invoice_p_total_sum_l24m` decimal(20,2),
  `invoice_e_cnt_l24m` int,
  `invoice_e_cancel_cnt_l24m` int,
  `invoice_e_red_cnt_l24m` int,
  `invoice_e_total_sum_l24m` decimal(20,2),
  `invoice_r_cnt_l24m` int,
  `invoice_r_cancel_cnt_l24m` int,
  `invoice_r_red_cnt_l24m` int,
  `invoice_r_total_sum_l24m` decimal(20,2),
  `invoice_red_cnt_rate_l24m` decimal(20,2),
  `invoice_cancel_cnt_rate_l24m` decimal(20,2),
  `invoice_red_total_sum_rate_l24m` decimal(20,2),
  `invoice_cancel_total_sum_rate_l24m` decimal(20,2),
  `invoice_lt100_cnt_l24m` int,
  `invoice_lt100_cr_cnt_l24m` int,
  `invoice_lt100_total_sum_l24m` decimal(20,2),
  `invoice_lt1000_cnt_l24m` int,
  `invoice_lt1000_cr_cnt_l24m` int,
  `invoice_lt1000_total_sum_l24m` decimal(20,2),
  `invoice_lt2500_cnt_l24m` int,
  `invoice_lt2500_cr_cnt_l24m` int,
  `invoice_lt2500_total_sum_l24m` decimal(20,2),
  `invoice_lt5000_cnt_l24m` int,
  `invoice_lt5000_cr_cnt_l24m` int,
  `invoice_lt5000_total_sum_l24m` decimal(20,2),
  `invoice_lt10000_cnt_l24m` int,
  `invoice_lt10000_cr_cnt_l24m` int,
  `invoice_lt10000_total_sum_l24m` decimal(20,2),
  `invoice_gt10000_cnt_l24m` int,
  `invoice_gt10000_cr_cnt_l24m` int,
  `invoice_gt10000_total_sum_l24m` decimal(20,2),
  `invoice_detail_cnt_l24m` int,
  `invoice_detail_cr_cnt_l24m` int,
  `invoice_detail_total_sum_l24m` decimal(20,2),
  `buyer_cnt_l24m` int,
  `buyer_a_invoice_cnt_l24m` int,
  `buyer_b_invoice_cnt_l24m` int,
  `buyer_c_invoice_cnt_l24m` int,
  `buyer_f_invoice_cnt_l24m` int,
  `buyer_a_amt_sum_l24m` decimal(20,2),
  `buyer_b_amt_sum_l24m` decimal(20,2),
  `buyer_c_amt_sum_l24m` decimal(20,2),
  `buyer_f_amt_sum_l24m` decimal(20,2),
  `invoice_sc_cnt_ratio_6m` decimal(20,2),
  `invoice_sc_cancel_cnt_ratio_6m` decimal(20,2),
  `invoice_sc_red_cnt_ratio_6m` decimal(20,2),
  `invoice_sc_total_sum_ratio_6m` decimal(20,2),
  `invoice_cnt_ratio_6m` decimal(20,2),
  `invoice_total_sum_ratio_6m` decimal(20,2),
  `invoice_s_cnt_ratio_6m` decimal(20,2),
  `invoice_s_total_sum_ratio_6m` decimal(20,2),
  `invoice_c_cnt_ratio_6m` decimal(20,2),
  `invoice_c_total_sum_ratio_6m` decimal(20,2),
  `invoice_t_cnt_ratio_6m` decimal(20,2),
  `invoice_t_total_sum_ratio_6m` decimal(20,2),
  `invoice_p_cnt_ratio_6m` decimal(20,2),
  `invoice_p_total_sum_ratio_6m` decimal(20,2),
  `invoice_e_cnt_ratio_6m` decimal(20,2),
  `invoice_e_total_sum_ratio_6m` decimal(20,2),
  `invoice_r_cnt_ratio_6m` decimal(20,2),
  `invoice_r_total_sum_ratio_6m` decimal(20,2),
  `invoice_red_cnt_rate_ratio_6m` decimal(20,2),
  `invoice_cancel_cnt_rate_ratio_6m` decimal(20,2),
  `invoice_red_total_sum_rate_ratio_6m` decimal(20,2),
  `invoice_cancel_total_sum_rate_ratio_6m` decimal(20,2),
  `invoice_lt100_cnt_ratio_6m` decimal(20,2),
  `invoice_lt100_total_sum_ratio_6m` decimal(20,2),
  `invoice_lt1000_cnt_ratio_6m` decimal(20,2),
  `invoice_lt1000_total_sum_ratio_6m` decimal(20,2),
  `invoice_lt2500_cnt_ratio_6m` decimal(20,2),
  `invoice_lt2500_total_sum_ratio_6m` decimal(20,2),
  `invoice_lt5000_cnt_ratio_6m` decimal(20,2),
  `invoice_lt5000_total_sum_ratio_6m` decimal(20,2),
  `invoice_gt10000_cnt_ratio_6m` decimal(20,2),
  `invoice_gt10000_total_sum_ratio_6m` decimal(20,2),
  `invoice_detail_cnt_ratio_6m` decimal(20,2),
  `invoice_detail_total_sum_ratio_6m` decimal(20,2),
  `buyer_cnt_chg_rate_6m` decimal(20,2),
  `buyer_a_cnt_chg_rate_6m` decimal(20,2),
  `buyer_b_cnt_chg_rate_6m` decimal(20,2),
  `buyer_c_cnt_chg_rate_6m` decimal(20,2),
  `buyer_f_cnt_chg_rate_6m` decimal(20,2),
  `buyer_a_invoice_cnt_l12m_chg_rate_6m` decimal(20,2),
  `buyer_b_invoice_cnt_l12m_chg_rate_6m` decimal(20,2),
  `buyer_c_invoice_cnt_l12m_chg_rate_6m` decimal(20,2),
  `buyer_f_invoice_cnt_l12m_chg_rate_6m` decimal(20,2),
  `buyer_a_amt_sum_l12m_chg_rate_6m` decimal(20,2),
  `buyer_b_amt_sum_l12m_chg_rate_6m` decimal(20,2),
  `buyer_c_amt_sum_l12m_chg_rate_6m` decimal(20,2),
  `buyer_f_amt_sum_l12m_chg_rate_6m` decimal(20,2),
  `invoice_sc_cnt_ratio_12m` decimal(20,2),
  `invoice_sc_cancel_cnt_ratio_12m` decimal(20,2),
  `invoice_sc_red_cnt_ratio_12m` decimal(20,2),
  `invoice_sc_total_sum_ratio_12m` decimal(20,2),
  `invoice_cnt_ratio_12m` decimal(20,2),
  `invoice_total_sum_ratio_12m` decimal(20,2),
  `invoice_s_cnt_ratio_12m` decimal(20,2),
  `invoice_s_total_sum_ratio_12m` decimal(20,2),
  `invoice_c_cnt_ratio_12m` decimal(20,2),
  `invoice_c_total_sum_ratio_12m` decimal(20,2),
  `invoice_t_cnt_ratio_12m` decimal(20,2),
  `invoice_t_total_sum_ratio_12m` decimal(20,2),
  `invoice_p_cnt_ratio_12m` decimal(20,2),
  `invoice_p_total_sum_ratio_12m` decimal(20,2),
  `invoice_e_cnt_ratio_12m` decimal(20,2),
  `invoice_e_total_sum_ratio_12m` decimal(20,2),
  `invoice_r_cnt_ratio_12m` decimal(20,2),
  `invoice_r_total_sum_ratio_12m` decimal(20,2),
  `invoice_red_cnt_rate_ratio_12m` decimal(20,2),
  `invoice_cancel_cnt_rate_ratio_12m` decimal(20,2),
  `invoice_red_total_sum_rate_ratio_12m` decimal(20,2),
  `invoice_cancel_total_sum_rate_ratio_12m` decimal(20,2),
  `invoice_lt100_cnt_ratio_12m` decimal(20,2),
  `invoice_lt100_total_sum_ratio_12m` decimal(20,2),
  `invoice_lt1000_cnt_ratio_12m` decimal(20,2),
  `invoice_lt1000_total_sum_ratio_12m` decimal(20,2),
  `invoice_lt2500_cnt_ratio_12m` decimal(20,2),
  `invoice_lt2500_total_sum_ratio_12m` decimal(20,2),
  `invoice_lt5000_cnt_ratio_12m` decimal(20,2),
  `invoice_lt5000_total_sum_ratio_12m` decimal(20,2),
  `invoice_gt10000_cnt_ratio_12m` decimal(20,2),
  `invoice_gt10000_total_sum_ratio_12m` decimal(20,2),
  `invoice_detail_cnt_ratio_12m` decimal(20,2),
  `invoice_detail_total_sum_ratio_12m` decimal(20,2),
  `buyer_cnt_chg_rate_12m` decimal(20,2),
  `buyer_a_cnt_chg_rate_12m` decimal(20,2),
  `buyer_b_cnt_chg_rate_12m` decimal(20,2),
  `buyer_c_cnt_chg_rate_12m` decimal(20,2),
  `buyer_f_cnt_chg_rate_12m` decimal(20,2),
  `buyer_a_invoice_cnt_l12m_chg_rate_12m` decimal(20,2),
  `buyer_b_invoice_cnt_l12m_chg_rate_12m` decimal(20,2),
  `buyer_c_invoice_cnt_l12m_chg_rate_12m` decimal(20,2),
  `buyer_f_invoice_cnt_l12m_chg_rate_12m` decimal(20,2),
  `buyer_a_amt_sum_l12m_chg_rate_12m` decimal(20,2),
  `buyer_b_amt_sum_l12m_chg_rate_12m` decimal(20,2),
  `buyer_c_amt_sum_l12m_chg_rate_12m` decimal(20,2),
  `buyer_f_amt_sum_l12m_chg_rate_12m` decimal(20,2),
  `is_delete` string,
  `create_time` date,
  `create_user` string,
  `modify_time` timestamp,
  `modify_user` string)