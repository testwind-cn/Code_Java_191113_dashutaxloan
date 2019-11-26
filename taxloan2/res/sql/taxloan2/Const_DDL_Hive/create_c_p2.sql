
CREATE TABLE IF NOT EXISTS `${hivevar:DATABASE_DEST}.c_p2`(
	  `mcht_cd` string,
	  `month` string,
	  `invoice_r_all_cnt_l3m` bigint,
	  `invoice_c_all_cnt_l3m` bigint,
	  `invoice_total_r_sum_l3m` decimal(38,18),
	  `invoice_total_c_sum_l3m` decimal(38,18),
	  `invoice_r_all_cnt_l6m` bigint,
	  `invoice_c_all_cnt_l6m` bigint,
	  `invoice_total_r_sum_l6m` decimal(38,18),
	  `invoice_total_c_sum_l6m` decimal(38,18),
	  `invoice_r_all_cnt_l12m` bigint,
	  `invoice_c_all_cnt_l12m` bigint,
	  `invoice_total_r_sum_l12m` decimal(38,18),
	  `invoice_total_c_sum_l12m` decimal(38,18),
	  `invoice_r_all_cnt_l24m` bigint,
	  `invoice_c_all_cnt_l24m` bigint,
	  `invoice_total_r_sum_l24m` decimal(38,18),
	  `invoice_total_c_sum_l24m` decimal(38,18),
	  `invoice_r_all_cnt_l712m` bigint,
	  `invoice_c_all_cnt_l712m` bigint,
	  `invoice_cnt_l712m` bigint,
	  `invoice_cnt_l1324m` bigint,
	  `invoice_r_all_cnt_l1324m` bigint,
	  `invoice_c_all_cnt_l1324m` bigint,
	  `buyer_cnt_l712m` bigint,
	  `buyer_cnt_l1324m` bigint,
	  `invoice_sc_cnt_l3m` bigint,
	  `invoice_sc_cancel_cnt_l3m` bigint,
	  `invoice_sc_red_cnt_l3m` bigint,
	  `invoice_sc_total_sum_l3m` decimal(38,18),
	  `invoice_cnt_l3m` bigint,
	  `invoice_cr_cnt_l3m` bigint,
	  `invoice_total_sum_l3m` decimal(38,18),
	  `invoice_s_cnt_l3m` bigint,
	  `invoice_s_cancel_cnt_l3m` bigint,
	  `invoice_s_red_cnt_l3m` bigint,
	  `invoice_s_total_sum_l3m` decimal(38,18),
	  `invoice_c_cnt_l3m` bigint,
	  `invoice_c_cancel_cnt_l3m` bigint,
	  `invoice_c_red_cnt_l3m` bigint,
	  `invoice_c_total_sum_l3m` decimal(38,18),
	  `invoice_t_cnt_l3m` bigint,
	  `invoice_t_cancel_cnt_l3m` bigint,
	  `invoice_t_red_cnt_l3m` bigint,
	  `invoice_t_total_sum_l3m` decimal(38,18),
	  `invoice_p_cnt_l3m` bigint,
	  `invoice_p_cancel_cnt_l3m` bigint,
	  `invoice_p_red_cnt_l3m` bigint,
	  `invoice_p_total_sum_l3m` decimal(38,18),
	  `invoice_e_cnt_l3m` bigint,
	  `invoice_e_cancel_cnt_l3m` bigint,
	  `invoice_e_red_cnt_l3m` bigint,
	  `invoice_e_total_sum_l3m` decimal(38,18),
	  `invoice_r_cnt_l3m` bigint,
	  `invoice_r_cancel_cnt_l3m` bigint,
	  `invoice_r_red_cnt_l3m` bigint,
	  `invoice_r_total_sum_l3m` decimal(38,18),
	  `invoice_lt100_cnt_l3m` bigint,
	  `invoice_lt100_cr_cnt_l3m` bigint,
	  `invoice_lt100_total_sum_l3m` decimal(38,18),
	  `invoice_lt1000_cnt_l3m` bigint,
	  `invoice_lt1000_cr_cnt_l3m` bigint,
	  `invoice_lt1000_total_sum_l3m` decimal(38,18),
	  `invoice_lt2500_cnt_l3m` bigint,
	  `invoice_lt2500_cr_cnt_l3m` bigint,
	  `invoice_lt2500_total_sum_l3m` decimal(38,18),
	  `invoice_lt5000_cnt_l3m` bigint,
	  `invoice_lt5000_cr_cnt_l3m` bigint,
	  `invoice_lt5000_total_sum_l3m` decimal(38,18),
	  `invoice_lt10000_cnt_l3m` bigint,
	  `invoice_lt10000_cr_cnt_l3m` bigint,
	  `invoice_lt10000_total_sum_l3m` decimal(38,18),
	  `invoice_gt10000_cnt_l3m` bigint,
	  `invoice_gt10000_cr_cnt_l3m` bigint,
	  `invoice_gt10000_total_sum_l3m` decimal(38,18),
	  `invoice_detail_cnt_l3m` bigint,
	  `invoice_detail_cr_cnt_l3m` bigint,
	  `invoice_detail_total_sum_l3m` decimal(38,18),
	  `buyer_cnt_l3m` bigint,
	  `invoice_sc_cnt_l6m` bigint,
	  `invoice_sc_cancel_cnt_l6m` bigint,
	  `invoice_sc_red_cnt_l6m` bigint,
	  `invoice_sc_total_sum_l6m` decimal(38,18),
	  `invoice_cnt_l6m` bigint,
	  `invoice_cr_cnt_l6m` bigint,
	  `invoice_total_sum_l6m` decimal(38,18),
	  `invoice_s_cnt_l6m` bigint,
	  `invoice_s_cancel_cnt_l6m` bigint,
	  `invoice_s_red_cnt_l6m` bigint,
	  `invoice_s_total_sum_l6m` decimal(38,18),
	  `invoice_c_cnt_l6m` bigint,
	  `invoice_c_cancel_cnt_l6m` bigint,
	  `invoice_c_red_cnt_l6m` bigint,
	  `invoice_c_total_sum_l6m` decimal(38,18),
	  `invoice_t_cnt_l6m` bigint,
	  `invoice_t_cancel_cnt_l6m` bigint,
	  `invoice_t_red_cnt_l6m` bigint,
	  `invoice_t_total_sum_l6m` decimal(38,18),
	  `invoice_p_cnt_l6m` bigint,
	  `invoice_p_cancel_cnt_l6m` bigint,
	  `invoice_p_red_cnt_l6m` bigint,
	  `invoice_p_total_sum_l6m` decimal(38,18),
	  `invoice_e_cnt_l6m` bigint,
	  `invoice_e_cancel_cnt_l6m` bigint,
	  `invoice_e_red_cnt_l6m` bigint,
	  `invoice_e_total_sum_l6m` decimal(38,18),
	  `invoice_r_cnt_l6m` bigint,
	  `invoice_r_cancel_cnt_l6m` bigint,
	  `invoice_r_red_cnt_l6m` bigint,
	  `invoice_r_total_sum_l6m` decimal(38,18),
	  `invoice_lt100_cnt_l6m` bigint,
	  `invoice_lt100_cr_cnt_l6m` bigint,
	  `invoice_lt100_total_sum_l6m` decimal(38,18),
	  `invoice_lt1000_cnt_l6m` bigint,
	  `invoice_lt1000_cr_cnt_l6m` bigint,
	  `invoice_lt1000_total_sum_l6m` decimal(38,18),
	  `invoice_lt2500_cnt_l6m` bigint,
	  `invoice_lt2500_cr_cnt_l6m` bigint,
	  `invoice_lt2500_total_sum_l6m` decimal(38,18),
	  `invoice_lt5000_cnt_l6m` bigint,
	  `invoice_lt5000_cr_cnt_l6m` bigint,
	  `invoice_lt5000_total_sum_l6m` decimal(38,18),
	  `invoice_lt10000_cnt_l6m` bigint,
	  `invoice_lt10000_cr_cnt_l6m` bigint,
	  `invoice_lt10000_total_sum_l6m` decimal(38,18),
	  `invoice_gt10000_cnt_l6m` bigint,
	  `invoice_gt10000_cr_cnt_l6m` bigint,
	  `invoice_gt10000_total_sum_l6m` decimal(38,18),
	  `invoice_detail_cnt_l6m` bigint,
	  `invoice_detail_cr_cnt_l6m` bigint,
	  `invoice_detail_total_sum_l6m` decimal(38,18),
	  `buyer_cnt_l6m` bigint,
	  `buyer_a_invoice_cnt_l6m` bigint,
	  `buyer_b_invoice_cnt_l6m` bigint,
	  `buyer_c_invoice_cnt_l6m` bigint,
	  `buyer_f_invoice_cnt_l6m` bigint,
	  `buyer_a_amt_sum_l6m` decimal(30,2),
	  `buyer_b_amt_sum_l6m` decimal(30,2),
	  `buyer_c_amt_sum_l6m` decimal(30,2),
	  `buyer_f_amt_sum_l6m` decimal(30,2),
	  `invoice_sc_cnt_l12m` bigint,
	  `invoice_sc_cancel_cnt_l12m` bigint,
	  `invoice_sc_red_cnt_l12m` bigint,
	  `invoice_sc_total_sum_l12m` decimal(38,18),
	  `invoice_cnt_l12m` bigint,
	  `invoice_cr_cnt_l12m` bigint,
	  `invoice_total_sum_l12m` decimal(38,18),
	  `invoice_s_cnt_l12m` bigint,
	  `invoice_s_cancel_cnt_l12m` bigint,
	  `invoice_s_red_cnt_l12m` bigint,
	  `invoice_s_total_sum_l12m` decimal(38,18),
	  `invoice_c_cnt_l12m` bigint,
	  `invoice_c_cancel_cnt_l12m` bigint,
	  `invoice_c_red_cnt_l12m` bigint,
	  `invoice_c_total_sum_l12m` decimal(38,18),
	  `invoice_t_cnt_l12m` bigint,
	  `invoice_t_cancel_cnt_l12m` bigint,
	  `invoice_t_red_cnt_l12m` bigint,
	  `invoice_t_total_sum_l12m` decimal(38,18),
	  `invoice_p_cnt_l12m` bigint,
	  `invoice_p_cancel_cnt_l12m` bigint,
	  `invoice_p_red_cnt_l12m` bigint,
	  `invoice_p_total_sum_l12m` decimal(38,18),
	  `invoice_e_cnt_l12m` bigint,
	  `invoice_e_cancel_cnt_l12m` bigint,
	  `invoice_e_red_cnt_l12m` bigint,
	  `invoice_e_total_sum_l12m` decimal(38,18),
	  `invoice_r_cnt_l12m` bigint,
	  `invoice_r_cancel_cnt_l12m` bigint,
	  `invoice_r_red_cnt_l12m` bigint,
	  `invoice_r_total_sum_l12m` decimal(38,18),
	  `invoice_lt100_cnt_l12m` bigint,
	  `invoice_lt100_cr_cnt_l12m` bigint,
	  `invoice_lt100_total_sum_l12m` decimal(38,18),
	  `invoice_lt1000_cnt_l12m` bigint,
	  `invoice_lt1000_cr_cnt_l12m` bigint,
	  `invoice_lt1000_total_sum_l12m` decimal(38,18),
	  `invoice_lt2500_cnt_l12m` bigint,
	  `invoice_lt2500_cr_cnt_l12m` bigint,
	  `invoice_lt2500_total_sum_l12m` decimal(38,18),
	  `invoice_lt5000_cnt_l12m` bigint,
	  `invoice_lt5000_cr_cnt_l12m` bigint,
	  `invoice_lt5000_total_sum_l12m` decimal(38,18),
	  `invoice_lt10000_cnt_l12m` bigint,
	  `invoice_lt10000_cr_cnt_l12m` bigint,
	  `invoice_lt10000_total_sum_l12m` decimal(38,18),
	  `invoice_gt10000_cnt_l12m` bigint,
	  `invoice_gt10000_cr_cnt_l12m` bigint,
	  `invoice_gt10000_total_sum_l12m` decimal(38,18),
	  `invoice_detail_cnt_l12m` bigint,
	  `invoice_detail_cr_cnt_l12m` bigint,
	  `invoice_detail_total_sum_l12m` decimal(38,18),
	  `buyer_cnt_l12m` bigint,
	  `buyer_a_invoice_cnt_l12m` bigint,
	  `buyer_b_invoice_cnt_l12m` bigint,
	  `buyer_c_invoice_cnt_l12m` bigint,
	  `buyer_f_invoice_cnt_l12m` bigint,
	  `buyer_a_amt_sum_l12m` decimal(30,2),
	  `buyer_b_amt_sum_l12m` decimal(30,2),
	  `buyer_c_amt_sum_l12m` decimal(30,2),
	  `buyer_f_amt_sum_l12m` decimal(30,2),
	  `invoice_sc_cnt_l24m` bigint,
	  `invoice_sc_cancel_cnt_l24m` bigint,
	  `invoice_sc_red_cnt_l24m` bigint,
	  `invoice_sc_total_sum_l24m` decimal(38,18),
	  `invoice_cnt_l24m` bigint,
	  `invoice_cr_cnt_l24m` bigint,
	  `invoice_total_sum_l24m` decimal(38,18),
	  `invoice_s_cnt_l24m` bigint,
	  `invoice_s_cancel_cnt_l24m` bigint,
	  `invoice_s_red_cnt_l24m` bigint,
	  `invoice_s_total_sum_l24m` decimal(38,18),
	  `invoice_c_cnt_l24m` bigint,
	  `invoice_c_cancel_cnt_l24m` bigint,
	  `invoice_c_red_cnt_l24m` bigint,
	  `invoice_c_total_sum_l24m` decimal(38,18),
	  `invoice_t_cnt_l24m` bigint,
	  `invoice_t_cancel_cnt_l24m` bigint,
	  `invoice_t_red_cnt_l24m` bigint,
	  `invoice_t_total_sum_l24m` decimal(38,18),
	  `invoice_p_cnt_l24m` bigint,
	  `invoice_p_cancel_cnt_l24m` bigint,
	  `invoice_p_red_cnt_l24m` bigint,
	  `invoice_p_total_sum_l24m` decimal(38,18),
	  `invoice_e_cnt_l24m` bigint,
	  `invoice_e_cancel_cnt_l24m` bigint,
	  `invoice_e_red_cnt_l24m` bigint,
	  `invoice_e_total_sum_l24m` decimal(38,18),
	  `invoice_r_cnt_l24m` bigint,
	  `invoice_r_cancel_cnt_l24m` bigint,
	  `invoice_r_red_cnt_l24m` bigint,
	  `invoice_r_total_sum_l24m` decimal(38,18),
	  `invoice_lt100_cnt_l24m` bigint,
	  `invoice_lt100_cr_cnt_l24m` bigint,
	  `invoice_lt100_total_sum_l24m` decimal(38,18),
	  `invoice_lt1000_cnt_l24m` bigint,
	  `invoice_lt1000_cr_cnt_l24m` bigint,
	  `invoice_lt1000_total_sum_l24m` decimal(38,18),
	  `invoice_lt2500_cnt_l24m` bigint,
	  `invoice_lt2500_cr_cnt_l24m` bigint,
	  `invoice_lt2500_total_sum_l24m` decimal(38,18),
	  `invoice_lt5000_cnt_l24m` bigint,
	  `invoice_lt5000_cr_cnt_l24m` bigint,
	  `invoice_lt5000_total_sum_l24m` decimal(38,18),
	  `invoice_lt10000_cnt_l24m` bigint,
	  `invoice_lt10000_cr_cnt_l24m` bigint,
	  `invoice_lt10000_total_sum_l24m` decimal(38,18),
	  `invoice_gt10000_cnt_l24m` bigint,
	  `invoice_gt10000_cr_cnt_l24m` bigint,
	  `invoice_gt10000_total_sum_l24m` decimal(38,18),
	  `invoice_detail_cnt_l24m` bigint,
	  `invoice_detail_cr_cnt_l24m` bigint,
	  `invoice_detail_total_sum_l24m` decimal(38,18),
	  `buyer_cnt_l24m` bigint,
	  `buyer_a_invoice_cnt_l24m` bigint,
	  `buyer_b_invoice_cnt_l24m` bigint,
	  `buyer_c_invoice_cnt_l24m` bigint,
	  `buyer_f_invoice_cnt_l24m` bigint,
	  `buyer_a_amt_sum_l24m` decimal(30,2),
	  `buyer_b_amt_sum_l24m` decimal(30,2),
	  `buyer_c_amt_sum_l24m` decimal(30,2),
	  `buyer_f_amt_sum_l24m` decimal(30,2),
	  `buyer_a_cnt_chg_rate_6m` double,
	  `buyer_b_cnt_chg_rate_6m` double,
	  `buyer_c_cnt_chg_rate_6m` double,
	  `buyer_f_cnt_chg_rate_6m` double,
	  `buyer_a_invoice_cnt_l12m_chg_rate_6m` double,
	  `buyer_b_invoice_cnt_l12m_chg_rate_6m` double,
	  `buyer_c_invoice_cnt_l12m_chg_rate_6m` double,
	  `buyer_f_invoice_cnt_l12m_chg_rate_6m` double,
	  `buyer_a_amt_sum_l12m_chg_rate_6m` decimal(38,18),
	  `buyer_b_amt_sum_l12m_chg_rate_6m` decimal(38,18),
	  `buyer_c_amt_sum_l12m_chg_rate_6m` decimal(38,18),
	  `buyer_f_amt_sum_l12m_chg_rate_6m` decimal(38,18),
	  `buyer_a_cnt_chg_rate_12m` double,
	  `buyer_b_cnt_chg_rate_12m` double,
	  `buyer_c_cnt_chg_rate_12m` double,
	  `buyer_f_cnt_chg_rate_12m` double,
	  `buyer_a_invoice_cnt_l12m_chg_rate_12m` double,
	  `buyer_b_invoice_cnt_l12m_chg_rate_12m` double,
	  `buyer_c_invoice_cnt_l12m_chg_rate_12m` double,
	  `buyer_f_invoice_cnt_l12m_chg_rate_12m` double,
	  `buyer_a_amt_sum_l12m_chg_rate_12m` decimal(38,18),
	  `buyer_b_amt_sum_l12m_chg_rate_12m` decimal(38,18),
	  `buyer_c_amt_sum_l12m_chg_rate_12m` decimal(38,18),
	  `buyer_f_amt_sum_l12m_chg_rate_12m` decimal(38,18)
     )
