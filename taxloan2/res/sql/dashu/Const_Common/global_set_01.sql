
set hivevar:DATABASE_SRC=dm_taxloan2;
set hivevar:DATABASE_DEST=dm_taxloan2;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
use ${hivevar:DATABASE_DEST}
set hivevar:ADD_TAXNO=
