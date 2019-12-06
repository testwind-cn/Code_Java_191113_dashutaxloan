#!/bin/bash

echo "*************************************全量导入生产库中的表到hive dm_taxloan库中*******************************"

source ./0000_set_vars_input.sh

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
${JAR_PATH} --init \
--hivevar:DATABASE_SRC="${HIVE_SRC}" \
--hivevar:DATABASE_DEST="${HIVE_DEST}" \
--hiveusedb="${HIVE_DEST}"

PKG="dashu"
OBJ="sparkMain"

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
${JAR_PATH} --init \
--hivevar:DATABASE_SRC="${HIVE_SRC}" \
--hivevar:DATABASE_DEST="${HIVE_DEST}" \
--hiveusedb="${HIVE_DEST}"

