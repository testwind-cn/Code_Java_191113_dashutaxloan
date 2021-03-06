#!/bin/bash
### AUTHOR: zoupp
### EMAIL: zoupp@allinpay.com
### DATE: 2019/04/23
### DESC: taxloan-sqoop-export
### REV: 3.0
source /etc/profile
source /var/lib/hadoop-hdfs/.bash_profile


source ./0000_set_vars_input.sh

sed  's/${MYSQL_DB}'"/${MYSQL2_DB}/g" 0320_sql_before_taxloan_incre.sql | cat
echo -e "\n=========== MYSQL BEGIN ===========\n"
sed  's/${MYSQL_DB}'"/${MYSQL2_DB}/g" 0320_sql_before_taxloan_incre.sql | mysql -u"${MYSQL2_USER}" -p"${MYSQL2_PASS_S}" -h"${MYSQL2_IP}" -P"${MYSQL2_PORT}"
echo "=========== MYSQL OK ==========="

### e0502 counterparty
#mysql -u"${MYSQL2_USER}" -p"${MYSQL2_PASS_S}" -h"${MYSQL2_IP}" -P"${MYSQL2_PORT}" -e "TRUNCATE TABLE ${MYSQL2_DB}.counterparty;COMMIT;"
sudo -E -u admin sqoop export \
-D sqoop.export.records.per.statement=1000 \
--batch \
--hcatalog-database ${HIVE_DEST} \
--hcatalog-table counterparty_incre \
--fields-terminated-by "\0001" \
--connect ${MYSQL2_URL} \
--username ${MYSQL2_USER} \
--password "${MYSQL2_PASS_S}" \
--m 1 \
--update-mode allowinsert \
--update-key "mcht_cd,data_month,buyer_name,buyer_tax_cd" \
--table counterparty \
--columns="mcht_cd,data_month,buyer_name,buyer_tax_cd,buyer_invoice_cnt,buyer_invoice_cr_cnt,buyer_invoice_amt_sum,buyer_invoice_tax_sum,buyer_invoice_total_sum,is_delete,create_time,create_user,modify_time,modify_user"


### e0503 counterparty_classify
#mysql -u"${MYSQL2_USER}" -p"${MYSQL2_PASS_S}" -h"${MYSQL2_IP}" -P"${MYSQL2_PORT}" -e "TRUNCATE TABLE ${MYSQL2_DB}.counterparty_classify;COMMIT;"
sudo -E -u admin sqoop export \
-D sqoop.export.records.per.statement=1000 \
--batch \
--hcatalog-database ${HIVE_DEST} \
--hcatalog-table counterparty_classify_incre \
--fields-terminated-by "\0001" \
--connect ${MYSQL2_URL} \
--username ${MYSQL2_USER} \
--password "${MYSQL2_PASS_S}" \
--m 1 \
--update-mode allowinsert \
--update-key "mcht_cd,data_month,buyer_name,buyer_tax_cd" \
--table counterparty_classify \
--columns="mcht_cd,data_month,buyer_name,buyer_tax_cd,buyer_hist_month,buyer_invoice_cnt_l24m,buyer_invoice_total_sum_l24m,buyer_invoice_cnt_l12m,buyer_invoice_total_sum_l12m,buyer_invoice_cnt_l6m,buyer_invoice_total_sum_l6m,buyer_invoice_cnt_l3m,buyer_invoice_total_sum_l3m,buyer_invoice_cnt_l712m,buyer_invoice_total_sum_l712m,buyer_invoice_cnt_l1324m,buyer_invoice_total_sum_l1324m,buyer_invoice_cnt_l1m, buyer_invoice_total_sum_l1m,buyer_type,is_delete,create_time,create_user,modify_time,modify_user"



### e0504 statistics_month
#mysql -u"${MYSQL2_USER}" -p"${MYSQL2_PASS_S}" -h"${MYSQL2_IP}" -P"${MYSQL2_PORT}" -e "TRUNCATE TABLE ${MYSQL2_DB}.statistics_month;COMMIT;"
sudo -E -u admin sqoop export \
-D sqoop.export.records.per.statement=1000 \
--batch \
--hcatalog-database ${HIVE_DEST} \
--hcatalog-table statistics_month_incre \
--fields-terminated-by "\0001" \
--connect ${MYSQL2_URL} \
--username ${MYSQL2_USER} \
--password "${MYSQL2_PASS_S}" \
--m 1 \
--update-mode allowinsert \
--update-key "mcht_cd,data_month" \
--table statistics_month \
--columns="mcht_cd,data_month,invoice_sc_cnt,invoice_sc_cancel_cnt,invoice_sc_red_cnt,invoice_sc_total_sum,invoice_cnt,invoice_cr_cnt,invoice_amt_sum,invoice_tax_sum,invoice_total_sum,invoice_cancel_cnt,invoice_red_cnt,invoice_total_c_sum,invoice_total_r_sum,invoice_s_cnt,invoice_s_cancel_cnt,invoice_s_red_cnt,invoice_s_amt_sum,invoice_s_tax_sum,invoice_s_total_sum, invoice_c_cnt,invoice_c_cancel_cnt,invoice_c_red_cnt,invoice_c_amt_sum,invoice_c_tax_sum,invoice_c_total_sum,invoice_t_cnt,invoice_t_cancel_cnt,invoice_t_red_cnt,invoice_t_amt_sum,invoice_t_tax_sum,invoice_t_total_sum,invoice_p_cnt,invoice_p_cancel_cnt,invoice_p_red_cnt,invoice_p_amt_sum,invoice_p_tax_sum,invoice_p_total_sum,invoice_e_cnt,invoice_e_cancel_cnt,invoice_e_red_cnt,invoice_e_amt_sum,invoice_e_tax_sum,invoice_e_total_sum,invoice_r_cnt,invoice_r_cancel_cnt,invoice_r_red_cnt,invoice_r_amt_sum,invoice_r_tax_sum,invoice_r_total_sum,invoice_red_cnt_rate,invoice_cancel_cnt_rate,invoice_red_total_sum_rate,invoice_cancel_total_sum_rate,invoice_lt100_cnt,invoice_lt100_cr_cnt,invoice_lt100_total_sum,invoice_lt1000_cnt,invoice_lt1000_cr_cnt,invoice_lt1000_total_sum,invoice_lt2500_cnt,invoice_lt2500_cr_cnt,invoice_lt2500_total_sum,invoice_lt5000_cnt,invoice_lt5000_cr_cnt,invoice_lt5000_total_sum,invoice_lt10000_cnt,invoice_lt10000_cr_cnt,invoice_lt10000_total_sum,invoice_gt10000_cnt,invoice_gt10000_cr_cnt,invoice_gt10000_total_sum,invoice_detail_cnt,invoice_detail_cr_cnt,invoice_detail_total_sum,buyer_cnt,buyer_cnt_all,buyer_a_cnt,buyer_b_cnt,buyer_c_cnt,buyer_f_cnt,is_delete,create_time,create_user,modify_time,modify_user"  



### e0505 statistics_crossmonth
#mysql -u"${MYSQL2_USER}" -p"${MYSQL2_PASS_S}" -h"${MYSQL2_IP}" -P"${MYSQL2_PORT}" -e "TRUNCATE TABLE ${MYSQL2_DB}.statistics_crossmonth;COMMIT;"
sudo -E -u admin sqoop export \
-D sqoop.export.records.per.statement=1000 \
--batch \
--hcatalog-database ${HIVE_DEST} \
--hcatalog-table statistics_crossmonth_incre \
--fields-terminated-by "\0001" \
--connect ${MYSQL2_URL} \
--username ${MYSQL2_USER} \
--password "${MYSQL2_PASS_S}" \
--m 1 \
--update-mode allowinsert \
--update-key "mcht_cd,data_month" \
--table statistics_crossmonth \
--columns="mcht_cd,data_month,invoice_sc_cnt_l3m,invoice_sc_cancel_cnt_l3m,invoice_sc_red_cnt_l3m,invoice_sc_total_sum_l3m,invoice_cnt_l3m,invoice_cr_cnt_l3m,invoice_total_sum_l3m,invoice_s_cnt_l3m,invoice_s_cancel_cnt_l3m,invoice_s_red_cnt_l3m,invoice_s_total_sum_l3m,invoice_c_cnt_l3m,invoice_c_cancel_cnt_l3m,invoice_c_red_cnt_l3m,invoice_c_total_sum_l3m,invoice_t_cnt_l3m,invoice_t_cancel_cnt_l3m,invoice_t_red_cnt_l3m,invoice_t_total_sum_l3m,invoice_p_cnt_l3m,invoice_p_cancel_cnt_l3m,invoice_p_red_cnt_l3m,invoice_p_total_sum_l3m,invoice_e_cnt_l3m,invoice_e_cancel_cnt_l3m,invoice_e_red_cnt_l3m,invoice_e_total_sum_l3m,invoice_r_cnt_l3m,invoice_r_cancel_cnt_l3m,invoice_r_red_cnt_l3m,invoice_r_total_sum_l3m,invoice_red_cnt_rate_l3m,invoice_cancel_cnt_rate_l3m,invoice_red_total_sum_rate_l3m,invoice_cancel_total_sum_rate_l3m,invoice_lt100_cnt_l3m,invoice_lt100_cr_cnt_l3m,invoice_lt100_total_sum_l3m,invoice_lt1000_cnt_l3m,invoice_lt1000_cr_cnt_l3m,invoice_lt1000_total_sum_l3m,invoice_lt2500_cnt_l3m,invoice_lt2500_cr_cnt_l3m,invoice_lt2500_total_sum_l3m,invoice_lt5000_cnt_l3m,invoice_lt5000_cr_cnt_l3m,invoice_lt5000_total_sum_l3m,invoice_lt10000_cnt_l3m,invoice_lt10000_cr_cnt_l3m,invoice_lt10000_total_sum_l3m,invoice_gt10000_cnt_l3m,invoice_gt10000_cr_cnt_l3m,invoice_gt10000_total_sum_l3m,invoice_detail_cnt_l3m,invoice_detail_cr_cnt_l3m,invoice_detail_total_sum_l3m,buyer_cnt_l3m,invoice_sc_cnt_l6m,invoice_sc_cancel_cnt_l6m,invoice_sc_red_cnt_l6m,invoice_sc_total_sum_l6m,invoice_cnt_l6m,invoice_cr_cnt_l6m,invoice_total_sum_l6m,invoice_s_cnt_l6m,invoice_s_cancel_cnt_l6m,invoice_s_red_cnt_l6m,invoice_s_total_sum_l6m,invoice_c_cnt_l6m,invoice_c_cancel_cnt_l6m,invoice_c_red_cnt_l6m,invoice_c_total_sum_l6m,invoice_t_cnt_l6m,invoice_t_cancel_cnt_l6m,invoice_t_red_cnt_l6m,invoice_t_total_sum_l6m,invoice_p_cnt_l6m,invoice_p_cancel_cnt_l6m,invoice_p_red_cnt_l6m,invoice_p_total_sum_l6m,invoice_e_cnt_l6m,invoice_e_cancel_cnt_l6m,invoice_e_red_cnt_l6m,invoice_e_total_sum_l6m,invoice_r_cnt_l6m,invoice_r_cancel_cnt_l6m,invoice_r_red_cnt_l6m,invoice_r_total_sum_l6m,invoice_red_cnt_rate_l6m,invoice_cancel_cnt_rate_l6m,invoice_red_total_sum_rate_l6m,invoice_cancel_total_sum_rate_l6m,invoice_lt100_cnt_l6m,invoice_lt100_cr_cnt_l6m,invoice_lt100_total_sum_l6m,invoice_lt1000_cnt_l6m,invoice_lt1000_cr_cnt_l6m,invoice_lt1000_total_sum_l6m,invoice_lt2500_cnt_l6m,invoice_lt2500_cr_cnt_l6m,invoice_lt2500_total_sum_l6m,invoice_lt5000_cnt_l6m,invoice_lt5000_cr_cnt_l6m,invoice_lt5000_total_sum_l6m,invoice_lt10000_cnt_l6m,invoice_lt10000_cr_cnt_l6m,invoice_lt10000_total_sum_l6m,invoice_gt10000_cnt_l6m,invoice_gt10000_cr_cnt_l6m,invoice_gt10000_total_sum_l6m,invoice_detail_cnt_l6m,invoice_detail_cr_cnt_l6m,invoice_detail_total_sum_l6m,buyer_cnt_l6m,buyer_a_invoice_cnt_l6m,buyer_b_invoice_cnt_l6m,buyer_c_invoice_cnt_l6m,buyer_f_invoice_cnt_l6m,buyer_a_amt_sum_l6m,buyer_b_amt_sum_l6m,buyer_c_amt_sum_l6m,buyer_f_amt_sum_l6m,invoice_sc_cnt_l12m,invoice_sc_cancel_cnt_l12m,invoice_sc_red_cnt_l12m,invoice_sc_total_sum_l12m,invoice_cnt_l12m,invoice_cr_cnt_l12m,invoice_total_sum_l12m,invoice_s_cnt_l12m,invoice_s_cancel_cnt_l12m,invoice_s_red_cnt_l12m,invoice_s_total_sum_l12m,invoice_c_cnt_l12m,invoice_c_cancel_cnt_l12m,invoice_c_red_cnt_l12m,invoice_c_total_sum_l12m,invoice_t_cnt_l12m,invoice_t_cancel_cnt_l12m,invoice_t_red_cnt_l12m,invoice_t_total_sum_l12m,invoice_p_cnt_l12m,invoice_p_cancel_cnt_l12m,invoice_p_red_cnt_l12m,invoice_p_total_sum_l12m,invoice_e_cnt_l12m,invoice_e_cancel_cnt_l12m,invoice_e_red_cnt_l12m,invoice_e_total_sum_l12m,invoice_r_cnt_l12m,invoice_r_cancel_cnt_l12m,invoice_r_red_cnt_l12m,invoice_r_total_sum_l12m,invoice_red_cnt_rate_l12m,invoice_cancel_cnt_rate_l12m,invoice_red_total_sum_rate_l12m,invoice_cancel_total_sum_rate_l12m,invoice_lt100_cnt_l12m,invoice_lt100_cr_cnt_l12m,invoice_lt100_total_sum_l12m,invoice_lt1000_cnt_l12m,invoice_lt1000_cr_cnt_l12m,invoice_lt1000_total_sum_l12m,invoice_lt2500_cnt_l12m,invoice_lt2500_cr_cnt_l12m,invoice_lt2500_total_sum_l12m,invoice_lt5000_cnt_l12m,invoice_lt5000_cr_cnt_l12m,invoice_lt5000_total_sum_l12m,invoice_lt10000_cnt_l12m,invoice_lt10000_cr_cnt_l12m,invoice_lt10000_total_sum_l12m,invoice_gt10000_cnt_l12m,invoice_gt10000_cr_cnt_l12m,invoice_gt10000_total_sum_l12m,invoice_detail_cnt_l12m,invoice_detail_cr_cnt_l12m,invoice_detail_total_sum_l12m,buyer_cnt_l12m,buyer_a_invoice_cnt_l12m,buyer_b_invoice_cnt_l12m,buyer_c_invoice_cnt_l12m,buyer_f_invoice_cnt_l12m,buyer_a_amt_sum_l12m,buyer_b_amt_sum_l12m,buyer_c_amt_sum_l12m,buyer_f_amt_sum_l12m,invoice_sc_cnt_l24m,invoice_sc_cancel_cnt_l24m,invoice_sc_red_cnt_l24m,invoice_sc_total_sum_l24m,invoice_cnt_l24m,invoice_cr_cnt_l24m,invoice_total_sum_l24m,invoice_s_cnt_l24m,invoice_s_cancel_cnt_l24m,invoice_s_red_cnt_l24m,invoice_s_total_sum_l24m,invoice_c_cnt_l24m,invoice_c_cancel_cnt_l24m,invoice_c_red_cnt_l24m,invoice_c_total_sum_l24m,invoice_t_cnt_l24m,invoice_t_cancel_cnt_l24m,invoice_t_red_cnt_l24m,invoice_t_total_sum_l24m,invoice_p_cnt_l24m,invoice_p_cancel_cnt_l24m,invoice_p_red_cnt_l24m,invoice_p_total_sum_l24m,invoice_e_cnt_l24m,invoice_e_cancel_cnt_l24m,invoice_e_red_cnt_l24m,invoice_e_total_sum_l24m,invoice_r_cnt_l24m,invoice_r_cancel_cnt_l24m,invoice_r_red_cnt_l24m,invoice_r_total_sum_l24m,invoice_red_cnt_rate_l24m,invoice_cancel_cnt_rate_l24m,invoice_red_total_sum_rate_l24m,invoice_cancel_total_sum_rate_l24m,invoice_lt100_cnt_l24m,invoice_lt100_cr_cnt_l24m,invoice_lt100_total_sum_l24m,invoice_lt1000_cnt_l24m,invoice_lt1000_cr_cnt_l24m,invoice_lt1000_total_sum_l24m,invoice_lt2500_cnt_l24m,invoice_lt2500_cr_cnt_l24m,invoice_lt2500_total_sum_l24m,invoice_lt5000_cnt_l24m,invoice_lt5000_cr_cnt_l24m,invoice_lt5000_total_sum_l24m,invoice_lt10000_cnt_l24m,invoice_lt10000_cr_cnt_l24m,invoice_lt10000_total_sum_l24m,invoice_gt10000_cnt_l24m,invoice_gt10000_cr_cnt_l24m,invoice_gt10000_total_sum_l24m,invoice_detail_cnt_l24m,invoice_detail_cr_cnt_l24m,invoice_detail_total_sum_l24m,buyer_cnt_l24m,buyer_a_invoice_cnt_l24m,buyer_b_invoice_cnt_l24m,buyer_c_invoice_cnt_l24m,buyer_f_invoice_cnt_l24m,buyer_a_amt_sum_l24m,buyer_b_amt_sum_l24m,buyer_c_amt_sum_l24m,buyer_f_amt_sum_l24m,invoice_sc_cnt_ratio_6m,invoice_sc_cancel_cnt_ratio_6m,invoice_sc_red_cnt_ratio_6m,invoice_sc_total_sum_ratio_6m,invoice_cnt_ratio_6m,invoice_total_sum_ratio_6m,invoice_s_cnt_ratio_6m,invoice_s_total_sum_ratio_6m,invoice_c_cnt_ratio_6m,invoice_c_total_sum_ratio_6m,invoice_t_cnt_ratio_6m,invoice_t_total_sum_ratio_6m,invoice_p_cnt_ratio_6m,invoice_p_total_sum_ratio_6m,invoice_e_cnt_ratio_6m,invoice_e_total_sum_ratio_6m,invoice_r_cnt_ratio_6m,invoice_r_total_sum_ratio_6m,invoice_red_cnt_rate_ratio_6m,invoice_cancel_cnt_rate_ratio_6m,invoice_red_total_sum_rate_ratio_6m,invoice_cancel_total_sum_rate_ratio_6m,invoice_lt100_cnt_ratio_6m,invoice_lt100_total_sum_ratio_6m,invoice_lt1000_cnt_ratio_6m,invoice_lt1000_total_sum_ratio_6m,invoice_lt2500_cnt_ratio_6m,invoice_lt2500_total_sum_ratio_6m,invoice_lt5000_cnt_ratio_6m,invoice_lt5000_total_sum_ratio_6m,invoice_gt10000_cnt_ratio_6m,invoice_gt10000_total_sum_ratio_6m,invoice_detail_cnt_ratio_6m,invoice_detail_total_sum_ratio_6m,buyer_cnt_chg_rate_6m,buyer_a_cnt_chg_rate_6m,buyer_b_cnt_chg_rate_6m,buyer_c_cnt_chg_rate_6m,buyer_f_cnt_chg_rate_6m,buyer_a_invoice_cnt_l12m_chg_rate_6m,buyer_b_invoice_cnt_l12m_chg_rate_6m,buyer_c_invoice_cnt_l12m_chg_rate_6m,buyer_f_invoice_cnt_l12m_chg_rate_6m,buyer_a_amt_sum_l12m_chg_rate_6m,buyer_b_amt_sum_l12m_chg_rate_6m,buyer_c_amt_sum_l12m_chg_rate_6m,buyer_f_amt_sum_l12m_chg_rate_6m,invoice_sc_cnt_ratio_12m,invoice_sc_cancel_cnt_ratio_12m,invoice_sc_red_cnt_ratio_12m,invoice_sc_total_sum_ratio_12m,invoice_cnt_ratio_12m,invoice_total_sum_ratio_12m,invoice_s_cnt_ratio_12m,invoice_s_total_sum_ratio_12m,invoice_c_cnt_ratio_12m,invoice_c_total_sum_ratio_12m,invoice_t_cnt_ratio_12m,invoice_t_total_sum_ratio_12m,invoice_p_cnt_ratio_12m,invoice_p_total_sum_ratio_12m,invoice_e_cnt_ratio_12m,invoice_e_total_sum_ratio_12m,invoice_r_cnt_ratio_12m,invoice_r_total_sum_ratio_12m,invoice_red_cnt_rate_ratio_12m,invoice_cancel_cnt_rate_ratio_12m,invoice_red_total_sum_rate_ratio_12m,invoice_cancel_total_sum_rate_ratio_12m,invoice_lt100_cnt_ratio_12m,invoice_lt100_total_sum_ratio_12m,invoice_lt1000_cnt_ratio_12m,invoice_lt1000_total_sum_ratio_12m,invoice_lt2500_cnt_ratio_12m,invoice_lt2500_total_sum_ratio_12m,invoice_lt5000_cnt_ratio_12m,invoice_lt5000_total_sum_ratio_12m,invoice_gt10000_cnt_ratio_12m,invoice_gt10000_total_sum_ratio_12m,invoice_detail_cnt_ratio_12m,invoice_detail_total_sum_ratio_12m,buyer_cnt_chg_rate_12m,buyer_a_cnt_chg_rate_12m,buyer_b_cnt_chg_rate_12m,buyer_c_cnt_chg_rate_12m,buyer_f_cnt_chg_rate_12m,buyer_a_invoice_cnt_l12m_chg_rate_12m,buyer_b_invoice_cnt_l12m_chg_rate_12m,buyer_c_invoice_cnt_l12m_chg_rate_12m,buyer_f_invoice_cnt_l12m_chg_rate_12m,buyer_a_amt_sum_l12m_chg_rate_12m,buyer_b_amt_sum_l12m_chg_rate_12m,buyer_c_amt_sum_l12m_chg_rate_12m,buyer_f_amt_sum_l12m_chg_rate_12m,is_delete,create_time,create_user,modify_time,modify_user"

### e0501 mcht_tax
#mysql -u"${MYSQL2_USER}" -p"${MYSQL2_PASS_S}" -h"${MYSQL2_IP}" -P"${MYSQL2_PORT}" -e "TRUNCATE TABLE ${MYSQL2_DB}.mcht_tax;COMMIT;"
sudo -E -u admin sqoop export \
-D sqoop.export.records.per.statement=1000 \
--batch \
--hcatalog-database ${HIVE_DEST} \
--hcatalog-table mcht_tax_incre \
--fields-terminated-by "\0001" \
--connect ${MYSQL2_URL} \
--username ${MYSQL2_USER} \
--password "${MYSQL2_PASS_S}" \
--m 1 \
--update-mode allowinsert \
--update-key "mcht_cd" \
--table mcht_tax \
--columns="mcht_cd,mcht_name,mcht_tax_cd,mcht_addtel,mcht_bankno,first_invoicedate,initial_get_flag,is_delete,create_time,create_user,modify_time,modify_user"


### e0506 saleinvoice
# mysql -u"${MYSQL2_USER}" -p"${MYSQL2_PASS_S}" -h"${MYSQL2_IP}" -P"${MYSQL2_PORT}" -e "TRUNCATE TABLE ${MYSQL2_DB}.saleinvoice;COMMIT;"
# sudo -E -u admin sqoop export \
# -D sqoop.export.records.per.statement=1000 \
# --batch \
# --hcatalog-database ${HIVE_DEST} \
# --hcatalog-table saleinvoice \
# --connect ${MYSQL2_URL} \
# --username ${MYSQL2_USER} \
# --password "${MYSQL2_PASS_S}" \
# --m 10 \
# --table saleinvoice_hive \
# --columns="cid,agentcode,authstate,authtime,authusername,batchid,billid,billno,buyeraddtel,buyerbankno,buyername,buyerno,buyertaxno,cancelflag,carriertaxno,cd,checkcode,checkresult,checkstate,checktime,ciphertext,cjhm,comments,confirmstate,createtime,detaillistflag,email,encryptionver,fdjhm,fhrsbh,forwordstate,forwordtime,gatheringperson,hgzh,imgstate,incomingstate,incomingtime,incomingusername,inputperson,invoicecode,invoicecontent,invoicedate,invoiceid,invoiceno,invoicetype,jztype,kpzdbs,logno,machineno,makeinvoicedeviceno,makeinvoiceperson,mobileno,pdfurl,printflag,pushstate,pushtime,qyd,recheckperson,repairflag,responecode,responeexplain,salebillno,selleraddtel,sellerbankno,sellername,sellertaxno,senterrtimes,sentoperator,senttime,senttobank,shrsbh,sktype,source,spsm,srctype,taxofficecode,taxrate,totalamount,totaltax,writebackstate,writebacktime,xcrs,xfdh,xfdz,yshwxx,yyzzh,zh,zyspmc"

sed  's/${MYSQL_DB}'"/${MYSQL2_DB}/g" 0320_sql_after_taxloan_incre.sql | cat
echo -e "\n=========== MYSQL BEGIN ===========\n"
sed  's/${MYSQL_DB}'"/${MYSQL2_DB}/g" 0320_sql_after_taxloan_incre.sql | mysql -u"${MYSQL2_USER}" -p"${MYSQL2_PASS_S}" -h"${MYSQL2_IP}" -P"${MYSQL2_PORT}"
echo "=========== MYSQL OK ==========="


DATE_S_NEW=$(cat "${PROJ_PATH}"newtime_"${HIVE_SRC}".txt 2>/dev/null)
echo "插入本次开始时间  ${DATE_S_NEW}"

# 日志表
echo  "${DATE_S_NEW}" >> "${PROJ_PATH}"oldtime_"${PKG}"_"${HIVE_DEST}".txt
THE_CMD="hive -e \"insert into ${HIVE_DEST}.control_table  select 'cjlog_last_${PKG}' as table_name, '${DATE_S_NEW}' as export_date\""
echo -e "\n\n========== 开始处理命令 ==========\n"
echo "su -p admin -c \"${THE_CMD}\""
su -p admin -c "${THE_CMD}"

