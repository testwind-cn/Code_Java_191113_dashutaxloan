#!/bin/bash
### AUTHOR: zoupp
### EMAIL: zoupp@allinpay.com
### DATE: 2019/05/08
### DESC: taxloan
### REV: 3.0
#source /etc/profile
#source /root/.bash_profile

source ./0000_set_vars_input.sh

DATE_S_NEW=$(cat "${PROJ_PATH}"newtime_"${HIVE_SRC}".txt 2>/dev/null)
DATE_S_OLD=$(sed -n '$p' "${PROJ_PATH}"oldtime_"${PKG}"_"${HIVE_DEST}".txt 2>/dev/null)

echo "开始时间  ${DATE_S_NEW}"
echo "上次时间  ${DATE_S_OLD}"

sudo -E -u admin spark-submit \
--class ${PKG}.${OBJ} \
--master yarn-client \
--num-executors ${NUM_EXE} \
--executor-cores ${EXE_CORE} \
--executor-memory ${EXE_MEM} \
--driver-memory 4g \
--conf spark.sql.shuffle.partitions=150 \
--name "${OBJ}" \
--jars "${JDBC_HOME}" \
${JAR_PATH} --incre --new="${DATE_S_NEW}" --old="${DATE_S_OLD}" \
--hivevar:DATABASE_SRC="${HIVE_SRC}" \
--hivevar:DATABASE_DEST="${HIVE_DEST}" \
--hiveusedb="${HIVE_DEST}"

