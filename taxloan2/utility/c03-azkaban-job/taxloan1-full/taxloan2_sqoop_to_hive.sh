#!/bin/bash

echo "*************************************全量导入生产库中的表到hive dm_taxloan库中*******************************"

source ./0000_set_vars_input.sh

DATE_S_NEW=$(date -d "0 day" "+%Y-%m-%d %H:%M:%S")

rm -rf "${PROJ_PATH}"time.txt

echo "${DATE_S_NEW}" > "${PROJ_PATH}"time.txt
echo "开始时间  ${DATE_S_NEW}"


# cjlog表
THE_CMD="sqoop import --connect ${URL} --username ${USER} --password-file=${PASS_F} --table cjlog --hive-delims-replacement ' ' --hive-import --hive-database ${HIVE_DB} --hive-table cjlog --hive-overwrite"
echo -e "\n\n========== 开始处理命令 ==========\n"
echo "su -p hdfs -c \"${THE_CMD}\""
su -p hdfs -c "${THE_CMD}"


# saleinvoice表
THE_CMD="sqoop import --connect ${URL} --username ${USER} --password-file=${PASS_F} --table saleinvoice --hive-delims-replacement ' ' --hive-import --hive-database ${HIVE_DB} --hive-table saleinvoice --hive-overwrite"
echo -e "\n\n========== 开始处理命令 ==========\n"
echo "su -p hdfs -c \"${THE_CMD}\""
su -p hdfs -c "${THE_CMD}"


# saleinvoicedetail表
THE_CMD="sqoop import --connect ${URL} --username ${USER} --password-file=${PASS_F} --table saleinvoicedetail --hive-delims-replacement ' ' --hive-import --hive-database ${HIVE_DB} --hive-table saleinvoicedetail --hive-overwrite"
echo -e "\n\n========== 开始处理命令 ==========\n"
echo "su -p hdfs -c \"${THE_CMD}\""
su -p hdfs -c "${THE_CMD}"


