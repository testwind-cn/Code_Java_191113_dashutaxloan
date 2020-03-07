set hivevar:DATABASE_DEST=dm_taxloan1;

DROP TABLE IF EXISTS ${hivevar:DATABASE_DEST}.bak_cjlog_tmp;
DROP TABLE IF EXISTS ${hivevar:DATABASE_DEST}.bak_cross_month_tmp;
DROP TABLE IF EXISTS ${hivevar:DATABASE_DEST}.bak_latest_month_tmp;
DROP TABLE IF EXISTS ${hivevar:DATABASE_DEST}.bak_saleinvoice_tmp;


CREATE TABLE IF NOT EXISTS ${hivevar:DATABASE_DEST}.bak_cjlog_tmp(
`taxno` string,
`oldtaxno` string
)
partitioned by (create_time string);


CREATE TABLE IF NOT EXISTS ${hivevar:DATABASE_DEST}.bak_cross_month_tmp (
  `mcht_cd` string,
  `month` string)
partitioned by (create_time string);

CREATE TABLE IF NOT EXISTS ${hivevar:DATABASE_DEST}.bak_latest_month_tmp(
  `mcht_cd` string,
  `month` string,
  `buyer_tax_cd` string,
  `buyername` string)
partitioned by (create_time string);


CREATE TABLE IF NOT EXISTS ${hivevar:DATABASE_DEST}.bak_saleinvoice_tmp (
  `sellertaxno` string,
  `sellername` string,
  `selleraddtel` string,
  `invoicedate` string,
  `data_month` string,
  `sellerbankno` string,
  `oldtaxno` string,
  `invoiceid` string,
  `buyername` string,
  `buyertaxno` string,
  `totalamount` double,
  `totaltax` double,
  `cancelflag` boolean,
  `invoicetype` string,
  `jztype` string)
partitioned by (create_time string);