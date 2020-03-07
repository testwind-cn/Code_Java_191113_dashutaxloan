#!/usr/bin/env bash

echo "$(cat /data/taxloan2/cleanData/cleanData.sql)"

sudo -u admin hive -e "$(cat /data/taxloan2/cleanData/cleanData.sql)"

sudo -u hdfs hdfs dfs -rm -r -f -skipTrash /user/admin/.Trash