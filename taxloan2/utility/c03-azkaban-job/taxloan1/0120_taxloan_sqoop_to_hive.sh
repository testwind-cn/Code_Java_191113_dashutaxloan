#!/bin/bash

echo "*************************************全量导入生产库中的表到hive dm_taxloan库中*******************************"

source ./0000_set_vars_input.sh

DATE_S_NEW=$(date -d "0 day" "+%Y-%m-%d %H:%M:%S")

rm -rf "${PROJ_PATH}"newtime_"${HIVE_SRC}".txt

echo "${DATE_S_NEW}" > "${PROJ_PATH}"newtime_"${HIVE_SRC}".txt
echo "开始时间  ${DATE_S_NEW}"

sudo -E -u hdfs hdfs dfs -mkdir -p /sqoop_tmp
sudo -E -u hdfs hdfs dfs -chown admin:hive /sqoop_tmp
sudo -E -u hdfs hdfs dfs -chmod 777 /sqoop_tmp
sudo -E -u hdfs hdfs dfs -rm -r -f -skipTrash /sqoop_tmp/${HIVE_SRC}
# sudo -E -u hdfs hdfs dfs -rm -r -f -skipTrash /sqoop_tmp/cjlog
# sudo -E -u hdfs hdfs dfs -rm -r -f -skipTrash /sqoop_tmp/saleinvoice
# sudo -E -u hdfs hdfs dfs -rm -r -f -skipTrash /sqoop_tmp/saleinvoicedetail

# cjlog表
THE_CMD="sqoop import --connect ${MYSQL1_URL} --username ${MYSQL1_USER} --password-file=${MYSQL1_PASS_F} --table cjlog --target-dir /sqoop_tmp/${HIVE_SRC} --delete-target-dir --hive-delims-replacement ' ' --hive-import --hive-database ${HIVE_SRC} --hive-table cjlog --hive-overwrite --temporary-rootdir /sqoop_tmp"
echo -e "\n\n========== 开始处理命令 ==========\n"
echo "su -p admin -c \"${THE_CMD}\""
su -p admin -c "${THE_CMD}"


# saleinvoice表
THE_CMD="sqoop import --connect ${MYSQL1_URL} --username ${MYSQL1_USER} --password-file=${MYSQL1_PASS_F} --table saleinvoice --target-dir /sqoop_tmp/${HIVE_SRC} --delete-target-dir --hive-delims-replacement ' ' --hive-import --hive-database ${HIVE_SRC} --hive-table saleinvoice --hive-overwrite --temporary-rootdir /sqoop_tmp"
echo -e "\n\n========== 开始处理命令 ==========\n"
echo "su -p admin -c \"${THE_CMD}\""
su -p admin -c "${THE_CMD}"


# saleinvoicedetail表
THE_CMD="sqoop import --connect ${MYSQL1_URL} --username ${MYSQL1_USER} --password-file=${MYSQL1_PASS_F} --table saleinvoicedetail --target-dir /sqoop_tmp/${HIVE_SRC} --delete-target-dir --hive-delims-replacement ' ' --hive-import --hive-database ${HIVE_SRC} --hive-table saleinvoicedetail --hive-overwrite --temporary-rootdir /sqoop_tmp"
echo -e "\n\n========== 开始处理命令 ==========\n"
echo "su -p admin -c \"${THE_CMD}\""
su -p admin -c "${THE_CMD}"

