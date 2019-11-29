CREATE TABLE IF NOT EXISTS ${hivevar:DATABASE_DEST}.p2 (
	  `mcht_cd` string,
	  `data_month` string,
	  `invoice_sc_cnt` bigint,
	  `invoice_sc_cancel_cnt` bigint,
	  `invoice_sc_red_cnt` bigint,
	  `invoice_sc_total_sum` double,
	  `invoice_cnt` bigint,
	  `invoice_cr_cnt` bigint,
	  `invoice_amt_sum` double,
	  `invoice_tax_sum` double,
	  `invoice_total_sum` double,
	  `invoice_cancel_cnt` bigint,
	  `invoice_red_cnt` bigint,
	  `invoice_total_c_sum` double,
	  `invoice_total_r_sum` double,
	  `invoice_s_cnt` bigint,
	  `invoice_s_cancel_cnt` bigint,
	  `invoice_s_red_cnt` bigint,
	  `invoice_s_amt_sum` double,
	  `invoice_s_tax_sum` double,
	  `invoice_s_total_sum` double,
	  `invoice_c_cnt` bigint,
	  `invoice_c_cancel_cnt` bigint,
	  `invoice_c_red_cnt` bigint,
	  `invoice_c_amt_sum` double,
	  `invoice_c_tax_sum` double,
	  `invoice_c_total_sum` double,
	  `invoice_t_cnt` bigint,
	  `invoice_t_cancel_cnt` bigint,
	  `invoice_t_red_cnt` bigint,
	  `invoice_t_amt_sum` double,
	  `invoice_t_tax_sum` double,
	  `invoice_t_total_sum` double,
	  `invoice_p_cnt` bigint,
	  `invoice_p_cancel_cnt` bigint,
	  `invoice_p_red_cnt` bigint,
	  `invoice_p_amt_sum` double,
	  `invoice_p_tax_sum` double,
	  `invoice_p_total_sum` double,
	  `invoice_e_cnt` bigint,
	  `invoice_e_cancel_cnt` bigint,
	  `invoice_e_red_cnt` bigint,
	  `invoice_e_amt_sum` double,
	  `invoice_e_tax_sum` double,
	  `invoice_e_total_sum` double,
	  `invoice_r_cnt` bigint,
	  `invoice_r_cancel_cnt` bigint,
	  `invoice_r_red_cnt` bigint,
	  `invoice_r_amt_sum` double,
	  `invoice_r_tax_sum` double,
	  `invoice_r_total_sum` double,
	  `invoice_red_cnt_rate` double,
	  `invoice_cancel_cnt_rate` double,
	  `invoice_red_total_sum_rate` double,
	  `invoice_cancel_total_sum_rate` double,
	  `invoice_lt100_cnt` bigint,
	  `invoice_lt100_cr_cnt` bigint,
	  `invoice_lt100_total_sum` double,
	  `invoice_lt1000_cnt` bigint,
	  `invoice_lt1000_cr_cnt` bigint,
	  `invoice_lt1000_total_sum` double,
	  `invoice_lt2500_cnt` bigint,
	  `invoice_lt2500_cr_cnt` bigint,
	  `invoice_lt2500_total_sum` double,
	  `invoice_lt5000_cnt` bigint,
	  `invoice_lt5000_cr_cnt` bigint,
	  `invoice_lt5000_total_sum` double,
	  `invoice_lt10000_cnt` bigint,
	  `invoice_lt10000_cr_cnt` bigint,
	  `invoice_lt10000_total_sum` double,
	  `invoice_gt10000_cnt` bigint,
	  `invoice_gt10000_cr_cnt` bigint,
	  `invoice_gt10000_total_sum` double,
	  `invoice_detail_cnt` bigint,
	  `invoice_detail_cr_cnt` bigint,
	  `invoice_detail_total_sum` double,
	  `buyer_cnt` bigint,
	  `buyer_cnt_all` bigint,
	  `buyer_a_cnt` bigint,
	  `buyer_b_cnt` bigint,
	  `buyer_c_cnt` bigint,
	  `buyer_f_cnt` bigint)