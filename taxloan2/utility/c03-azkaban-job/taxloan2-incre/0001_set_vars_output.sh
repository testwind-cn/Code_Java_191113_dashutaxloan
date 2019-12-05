#!/bin/bash


LOCAL_IP=$(ifconfig eth0 | grep 'inet' | awk -F ' ' '{print $2}')
LOCAL_NAME=$(hostname)

JDBC_HOME="${JAVA_HOME}/jre/lib/ext/mysql-connector-java-5.1.44-bin.jar"
JAR_PATH=/data/taxloan2/taxloan2.jar
NUM_EXE=15
EXE_CORE=4
EXE_MEM="16G"
PKG="taxloan2"
OBJ="sparkMain"

if [[ ${LOCAL_IP} = '10.91.1.100' ]]
then        # 10.91.1.100
    echo "=============== 在生产服务器设置变量 =================="

    MYSQL2_IP="172.31.100.202"
    MYSQL2_PORT="3306"
    MYSQL2_DB="data_warehouse"
    MYSQL2_URL="jdbc:mysql://${MYSQL2_IP}:${MYSQL2_PORT}/${MYSQL2_DB}"
    MYSQL2_USER="datauser"
    MYSQL2_PASS_F="/user/hive/warehouse/mysql_pwd_202"
    MYSQL2_PASS_S="kpo9raFllycl@fc0"
    HIVE_SRC="dm_taxloan2"
    HIVE_DEST="dm_taxloan2"
    PROJ_PATH="/data/taxloan2/"


else        # 10.91.1.21
    echo "=============== 在测试服务器设置变量 =================="


    MYSQL2_IP="10.91.1.19"
    MYSQL2_PORT="3306"
    MYSQL2_DB="data_warehouse2"
    MYSQL2_URL="jdbc:mysql://${MYSQL2_IP}:${MYSQL2_PORT}/${MYSQL2_DB}"
    MYSQL2_USER="root"
    MYSQL2_PASS_F="/user/hive/warehouse/mysql_pwd_19.txt"
    MYSQL2_PASS_S="Redhat@2016"
    HIVE_SRC="dm_taxloan2"
    HIVE_DEST="dm_taxloan2"
    PROJ_PATH="/data/taxloan2/"

fi



