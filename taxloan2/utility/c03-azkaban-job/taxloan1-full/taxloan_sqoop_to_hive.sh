#!/bin/bash

echo "*************************************全量导入生产库中的表到hive dm_taxloan库中*******************************"

source ./0000_set_vars_input.sh

DATE_S_NEW=$(date -d "0 day" "+%Y-%m-%d %H:%M:%S")

rm -rf "${PROJ_PATH}"time.txt

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
${JAR_PATH} --init

echo "${DATE_S_NEW}" > "${PROJ_PATH}"time.txt
echo "开始时间  ${DATE_S_NEW}"

sudo -E -u hdfs hdfs dfs -mkdir -p /sqoop_tmp
sudo -E -u hdfs hdfs dfs -chown admin:hive /sqoop_tmp
sudo -E -u hdfs hdfs dfs -chmod 777 /sqoop_tmp
sudo -E -u hdfs hdfs dfs -rm -r -f -skipTrash /sqoop_tmp/${HIVE_SRC}
# sudo -E -u hdfs hdfs dfs -rm -r -f -skipTrash /sqoop_tmp/cjlog
# sudo -E -u hdfs hdfs dfs -rm -r -f -skipTrash /sqoop_tmp/saleinvoice
# sudo -E -u hdfs hdfs dfs -rm -r -f -skipTrash /sqoop_tmp/saleinvoicedetail

# cjlog表
THE_CMD="sqoop import --connect ${URL} --username ${USER} --password-file=${PASS_F} --table cjlog --target-dir /sqoop_tmp/${HIVE_SRC} --delete-target-dir --hive-delims-replacement ' ' --hive-import --hive-database ${HIVE_SRC} --hive-table cjlog --hive-overwrite --temporary-rootdir /sqoop_tmp"
echo -e "\n\n========== 开始处理命令 ==========\n"
echo "su -p admin -c \"${THE_CMD}\""
su -p admin -c "${THE_CMD}"


# saleinvoice表
THE_CMD="sqoop import --connect ${URL} --username ${USER} --password-file=${PASS_F} --table saleinvoice --target-dir /sqoop_tmp/${HIVE_SRC} --delete-target-dir --hive-delims-replacement ' ' --hive-import --hive-database ${HIVE_SRC} --hive-table saleinvoice --hive-overwrite --temporary-rootdir /sqoop_tmp"
echo -e "\n\n========== 开始处理命令 ==========\n"
echo "su -p admin -c \"${THE_CMD}\""
su -p admin -c "${THE_CMD}"


# saleinvoicedetail表
THE_CMD="sqoop import --connect ${URL} --username ${USER} --password-file=${PASS_F} --table saleinvoicedetail --target-dir /sqoop_tmp/${HIVE_SRC} --delete-target-dir --hive-delims-replacement ' ' --hive-import --hive-database ${HIVE_SRC} --hive-table saleinvoicedetail --hive-overwrite --temporary-rootdir /sqoop_tmp"
echo -e "\n\n========== 开始处理命令 ==========\n"
echo "su -p admin -c \"${THE_CMD}\""
su -p admin -c "${THE_CMD}"


