
set hivevar:DATABASE_DEST=dm_taxloan2;
set hivevar:DATABASE_DEST=dm_taxloan2   ;
set hivevar:DATABASE_DEST=dm_taxloan2333 ----- sdasdsad ---ds sdadsasd
set hivevar:DATABASE_DEST=dm_taxloan233; ----- sdasdsad ---ds sdadsasd
       -- test
set hivevar:DATABASE_SRC=dm_taxloan1;
set hivevar:DATABASE_DEST=dm_taxloan1;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hivevar:DATABASE_USE=${hivevar:DATABASE_DEST}
