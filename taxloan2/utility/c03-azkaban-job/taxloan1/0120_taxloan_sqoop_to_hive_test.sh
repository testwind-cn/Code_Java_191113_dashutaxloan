#!/bin/bash

echo "*************************************全量导入生产库中的表到hive dm_taxloan库中*******************************"

source ./0000_set_vars_input.sh



# saleinvoice表
THE_CMD="sqoop import \
--connect ${MYSQL1_URL} \
--username ${MYSQL1_USER} \
--password-file=${MYSQL1_PASS_F} \
--table saleinvoice \
--target-dir /sqoop_tmp/${HIVE_SRC} \
--delete-target-dir \
--hive-delims-replacement ' ' \
--hive-import \
--hive-database ${HIVE_SRC} \
--hive-table saleinvoice \
--hive-overwrite \
--temporary-rootdir /sqoop_tmp"

THE_CMD="sqoop import \
--connect ${URL} \
--username ${USER} \
--password='${PASS_S}' \
--table saleinvoice \
--num-mappers 5 \
--delete-target-dir \
--target-dir=/user/hive/warehouse/${HIVE_DB}.tmp/saleinvoice \
--hive-import \
--hive-database ${HIVE_DB} \
--hive-table saleinvoice  \
--hive-delims-replacement ' ' \
--create-hive-table"

# --table saleinvoice \

THE_CMD="sqoop import \
--connect ${MYSQL1_URL} \
--username ${MYSQL1_USER} \
--password-file=${MYSQL1_PASS_F} \
--query \"SELECT * FROM invoice.saleinvoice WHERE createtime>'2019-12-07 00:00:00'\" \
--target-dir /sqoop_tmp/${HIVE_SRC} \
--delete-target-dir \
--hive-delims-replacement ' ' \
--hive-import \
--hive-database ${HIVE_SRC} \
--hive-table saleinvoice111 \
--hive-overwrite \
--temporary-rootdir /sqoop_tmp"


echo -e "\n\n========== 开始处理命令 ==========\n"
echo "su -p admin -c \"${THE_CMD}\""
su -p admin -c "${THE_CMD}"


