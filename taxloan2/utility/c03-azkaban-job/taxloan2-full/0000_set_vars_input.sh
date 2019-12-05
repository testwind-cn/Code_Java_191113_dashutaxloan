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

    MYSQL1_IP="172.31.100.202"
    MYSQL1_PORT="3306"
    MYSQL1_DB="invoice"
    MYSQL1_URL="jdbc:mysql://${MYSQL1_IP}:${MYSQL1_PORT}/${MYSQL1_DB}?useSSL=false"
    MYSQL1_USER="root"
    MYSQL1_PASS_F="/user/hive/warehouse/mysql_pwd_202"
    MYSQL1_PASS_S="9G3z8~a0JXa#8G9E"
    HIVE_SRC="dm_taxloan2"
    HIVE_DEST="dm_taxloan2"
    PROJ_PATH="/data/taxloan2/"


else        # 10.91.1.21
    echo "=============== 在测试服务器设置变量 =================="


    MYSQL1_IP="10.91.1.19"
    MYSQL1_IP="172.31.100.202"
    MYSQL1_PORT="3306"
    MYSQL1_DB="invoice"
    MYSQL1_URL="jdbc:mysql://${MYSQL1_IP}:${MYSQL1_PORT}/${MYSQL1_DB}?useSSL=false"
    MYSQL1_USER="root"
    MYSQL1_PASS_F="/user/hive/warehouse/mysql_pwd_19.txt"
    MYSQL1_PASS_F="/user/hive/warehouse/mysql_pwd_202"
    MYSQL1_PASS_S="Redhat@2016"
    MYSQL1_PASS_S="9G3z8~a0JXa#8G9E"
    HIVE_SRC="dm_taxloan2"
    HIVE_DEST="dm_taxloan2"
    PROJ_PATH="/data/taxloan2/"

fi



