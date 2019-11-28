#!/bin/bash
### AUTHOR: zoupp
### EMAIL: zoupp@allinpay.com
### DATE: 2019/05/08
### DESC: taxloan
### REV: 3.0
#source /etc/profile
#source /root/.bash_profile


JDBC_HOME="${JAVA_HOME}/jre/lib/ext/mysql-connector-java-5.1.44-bin.jar"
JAR_PATH=/data/taxloan/scripts/taxloan_2.0.jar
NUM_EXE=15
EXE_CORE=4
EXE_MEM="16G"
PKG="taxloan"
OBJ="taxloan_month"

spark-submit \
--class ${PKG}.${OBJ} \
--master yarn-client \
--num-executors ${NUM_EXE} \
--executor-cores ${EXE_CORE} \
--executor-memory ${EXE_MEM} \
--driver-memory 4g \
--conf spark.sql.shuffle.partitions=150 \
--name "${OBJ}" \
--jars "${JDBC_HOME}" \
${JAR_PATH}
