
IP="10.91.1.19"
PORT="3306"
MYSQL_DB="data_warehouse2"
URL="jdbc:mysql://${IP}:${PORT}/${MYSQL_DB}?useSSL=false"
USER="root"
PASS_F="/user/hive/warehouse/mysql_pwd_202"
PASS_S="Redhat@2016"
HIVE_DEST="dm_taxloan"



mysql -u"${USER}" -p"${PASS_S}" -h"${IP}" -P"${PORT}" -e "TRUNCATE TABLE ${MYSQL_DB}.deal_record; COMMIT;"

sudo -E -u hive sqoop export \
--hcatalog-database ${HIVE_DEST} \
--hcatalog-table deal_record \
--fields-terminated-by "\0001" \
--connect ${URL} \
--username ${USER} \
--password "${PASS_S}" \
--m 1 \
--update-mode allowinsert \
--update-key "taxno"  \
--table deal_record \
--columns="taxno,nondeal_days,invoice_date_min,invoice_date_max,nondeal_days_l6,nondeal_days_l12,nondeal_days_l24,create_user,modify_time, modify_user,create_time"


mysql -u"${USER}" -p"${PASS_S}" -h"${IP}" -P"${PORT}" -e "TRUNCATE TABLE ${MYSQL_DB}.down_customer_list; COMMIT;"

sudo -E -u hive sqoop export \
--hcatalog-database ${HIVE_DEST} \
--hcatalog-table down_customer_list \
--fields-terminated-by "\0001" \
--connect ${URL} \
--username ${USER} \
--password "${PASS_S}" \
--m 1 \
--update-mode allowinsert \
--update-key "taxno,quarter"  \
--table down_customer_list \
--columns="taxno,quarter,customer_num,top5sale_amount,top5sale_amount_ratio,top5deal_num,top5deal_num_ratio,create_user,modify_user,create_time,modify_time"



mysql -u"${USER}" -p"${PASS_S}" -h"${IP}" -P"${PORT}" -e "TRUNCATE TABLE ${MYSQL_DB}.sale_region_list; COMMIT;"

sudo -E -u hive sqoop export \
--hcatalog-database ${HIVE_DEST} \
--hcatalog-table sale_region_list \
--fields-terminated-by "\0001" \
--connect ${URL} \
--username ${USER} \
--password "${PASS_S}" \
--m 1 \
--update-mode allowinsert \
--update-key "taxno,year,rank"  \
--table sale_region_list \
--columns="taxno,year,rank,customer_province,deal_amount_region,deal_num_region,create_user,modify_user,create_time,modify_time"


mysql -u"${USER}" -p"${PASS_S}" -h"${IP}" -P"${PORT}" -e "TRUNCATE TABLE ${MYSQL_DB}.saleinvoice_month; COMMIT;"

sudo -E -u hive sqoop export \
--hcatalog-database ${HIVE_DEST} \
--hcatalog-table saleinvoice_month \
--fields-terminated-by "\0001" \
--connect ${URL} \
--username ${USER} \
--password "${PASS_S}" \
--m 1 \
--update-mode allowinsert \
--update-key "taxno,month"  \
--table saleinvoice_month \
--columns="taxno,month,taxsale_amount,taxsale_amount_zp,taxsale_amount_pp,tax_amount,red_amount,invalid_amount,valid_num,valid_num_zp,valid_num_pp,red_num,invalid_num,red_amount_ratio,invalid_amount_ratio,red_num_ratio,invalid_num_ratio,taxsale_amount_yoy,taxsale_amount_mom,create_user,modify_user,create_time,modify_time"
