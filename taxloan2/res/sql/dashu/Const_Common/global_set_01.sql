
set hivevar:DATABASE_SRC=dm_taxloan1;
set hivevar:DATABASE_DEST=dm_taxloan1;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hivevar:DATABASE_USE=${hivevar:DATABASE_DEST}
set hivevar:ADD_TAXNO=
