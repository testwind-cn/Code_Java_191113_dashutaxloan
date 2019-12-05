#!/bin/bash
### AUTHOR: zoupp
### EMAIL: zoupp@allinpay.com
### DATE: 2019/04/23
### DESC: taxloan-sqoop-export
### REV: 3.0
source /etc/profile
source /var/lib/hadoop-hdfs/.bash_profile


source ./0001_set_vars_output.sh

PKG="dashu"
OBJ="sparkMain"

sed  's/${MYSQL_DB}'"/${MYSQL2_DB}/g" sql_before_dashu.sql | cat
echo -e "\n=========== MYSQL BEGIN ===========\n"
sed  's/${MYSQL_DB}'"/${MYSQL2_DB}/g" sql_before_dashu.sql | mysql -u"${MYSQL2_USER}" -p"${MYSQL2_PASS_S}" -h"${MYSQL2_IP}" -P"${MYSQL2_PORT}"
echo "=========== MYSQL OK ==========="

### e0502 deal_record
#mysql -u"${MYSQL2_USER}" -p"${MYSQL2_PASS_S}" -h"${MYSQL2_IP}" -P"${MYSQL2_PORT}" -e "TRUNCATE TABLE ${MYSQL2_DB}.deal_record; COMMIT;"
sudo -E -u admin sqoop export \
-D sqoop.export.records.per.statement=1000 \
--batch \
--hcatalog-database ${HIVE_DEST} \
--hcatalog-table deal_record \
--fields-terminated-by "\0001" \
--connect ${MYSQL2_URL} \
--username ${MYSQL2_USER} \
--password "${MYSQL2_PASS_S}" \
--m 1 \
--update-mode allowinsert \
--update-key "taxno"  \
--table deal_record \
--columns="taxno,nondeal_days,invoice_date_min,invoice_date_max,nondeal_days_l6,nondeal_days_l12,nondeal_days_l24,create_user,modify_time, modify_user,create_time"


### e0503 down_customer_list
#mysql -u"${MYSQL2_USER}" -p"${MYSQL2_PASS_S}" -h"${MYSQL2_IP}" -P"${MYSQL2_PORT}" -e "TRUNCATE TABLE ${MYSQL2_DB}.down_customer_list; COMMIT;"
sudo -E -u admin sqoop export \
-D sqoop.export.records.per.statement=1000 \
--batch \
--hcatalog-database ${HIVE_DEST} \
--hcatalog-table down_customer_list \
--fields-terminated-by "\0001" \
--connect ${MYSQL2_URL} \
--username ${MYSQL2_USER} \
--password "${MYSQL2_PASS_S}" \
--m 1 \
--update-mode allowinsert \
--update-key "taxno,quarter"  \
--table down_customer_list \
--columns="taxno,quarter,customer_num,top5sale_amount,top5sale_amount_ratio,top5deal_num,top5deal_num_ratio,create_user,modify_user,create_time,modify_time"



### e0504 sale_region_list
#mysql -u"${MYSQL2_USER}" -p"${MYSQL2_PASS_S}" -h"${MYSQL2_IP}" -P"${MYSQL2_PORT}" -e "TRUNCATE TABLE ${MYSQL2_DB}.statistics_month;COMMIT;"
sudo -E -u admin sqoop export \
-D sqoop.export.records.per.statement=1000 \
--batch \
--hcatalog-database ${HIVE_DEST} \
--hcatalog-table sale_region_list \
--fields-terminated-by "\0001" \
--connect ${MYSQL2_URL} \
--username ${MYSQL2_USER} \
--password "${MYSQL2_PASS_S}" \
--m 1 \
--update-mode allowinsert \
--update-key "taxno,year,rank"  \
--table sale_region_list \
--columns="taxno,year,rank,customer_province,deal_amount_region,deal_num_region,create_user,modify_user,create_time,modify_time"



### e0505 saleinvoice_month
#mysql -u"${MYSQL2_USER}" -p"${MYSQL2_PASS_S}" -h"${MYSQL2_IP}" -P"${MYSQL2_PORT}" -e "TRUNCATE TABLE ${MYSQL2_DB}.saleinvoice_month; COMMIT;"
sudo -E -u admin sqoop export \
-D sqoop.export.records.per.statement=1000 \
--batch \
--hcatalog-database ${HIVE_DEST} \
--hcatalog-table saleinvoice_month \
--fields-terminated-by "\0001" \
--connect ${MYSQL2_URL} \
--username ${MYSQL2_USER} \
--password "${MYSQL2_PASS_S}" \
--m 1 \
--update-mode allowinsert \
--update-key "taxno,month"  \
--table saleinvoice_month \
--columns="taxno,month,taxsale_amount,taxsale_amount_zp,taxsale_amount_pp,tax_amount,red_amount,invalid_amount,valid_num,valid_num_zp,valid_num_pp,red_num,invalid_num,red_amount_ratio,invalid_amount_ratio,red_num_ratio,invalid_num_ratio,taxsale_amount_yoy,taxsale_amount_mom,create_user,modify_user,create_time,modify_time"




sed  's/${MYSQL_DB}'"/${MYSQL2_DB}/g" sql_after_dashu.sql | cat
echo -e "\n=========== MYSQL BEGIN ===========\n"
sed  's/${MYSQL_DB}'"/${MYSQL2_DB}/g" sql_after_dashu.sql | mysql -u"${MYSQL2_USER}" -p"${MYSQL2_PASS_S}" -h"${MYSQL2_IP}" -P"${MYSQL2_PORT}"
echo "=========== MYSQL OK ==========="


DATE_S_NEW=$(cat "${PROJ_PATH}"newtime_"${HIVE_SRC}".txt 2>/dev/null)
echo "插入本次开始时间  ${DATE_S_NEW}"

# 日志表
echo  "${DATE_S_NEW}" >> "${PROJ_PATH}"oldtime_"${PKG}"_"${HIVE_DEST}".txt
THE_CMD="hive -e \"insert into ${HIVE_DEST}.control_table  select 'cjlog_last_${PKG}' as table_name, '${DATE_S_NEW}' as export_date\""
echo -e "\n\n========== 开始处理命令 ==========\n"
echo "su -p admin -c \"${THE_CMD}\""
su -p admin -c "${THE_CMD}"

