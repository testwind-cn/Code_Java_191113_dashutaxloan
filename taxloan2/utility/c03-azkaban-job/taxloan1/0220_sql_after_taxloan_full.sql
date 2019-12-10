use ${MYSQL_DB};

-- -------------
ALTER TABLE ${MYSQL_DB}.mcht_tax_hive ADD INDEX idx_mcht_cd (mcht_cd) ;
ALTER TABLE ${MYSQL_DB}.mcht_tax_hive ADD INDEX idx_mcht_tax_cd (mcht_tax_cd) USING BTREE ;
ALTER TABLE ${MYSQL_DB}.mcht_tax_hive ADD UNIQUE index_mcht_cd (mcht_cd) ;

DROP TABLE IF EXISTS ${MYSQL_DB}.mcht_tax_last;
CREATE TABLE IF NOT EXISTS ${MYSQL_DB}.mcht_tax LIKE ${MYSQL_DB}.mcht_tax_hive;
RENAME TABLE ${MYSQL_DB}.mcht_tax to ${MYSQL_DB}.mcht_tax_last;
RENAME TABLE ${MYSQL_DB}.mcht_tax_hive to ${MYSQL_DB}.mcht_tax;
-- DROP TABLE IF EXISTS ${MYSQL_DB}.mcht_tax_last;
-- -------------

-- DROP TABLE IF EXISTS ${MYSQL_DB}.saleinvoice_last;
-- rename table ${MYSQL_DB}.saleinvoice to ${MYSQL_DB}.saleinvoice_last, ${MYSQL_DB}.saleinvoice_hive to ${MYSQL_DB}.saleinvoice;
-- DROP TABLE IF EXISTS ${MYSQL_DB}.saleinvoice_last;

-- -------------
ALTER TABLE ${MYSQL_DB}.counterparty_hive ADD INDEX idx_mcht_cd (mcht_cd) ;
ALTER TABLE ${MYSQL_DB}.counterparty_hive ADD INDEX idx_data_month (data_month) USING BTREE ;
ALTER TABLE ${MYSQL_DB}.counterparty_hive ADD UNIQUE index_unique (mcht_cd,data_month,buyer_name,buyer_tax_cd) ;

DROP TABLE IF EXISTS ${MYSQL_DB}.counterparty_last;
CREATE TABLE IF NOT EXISTS ${MYSQL_DB}.counterparty LIKE ${MYSQL_DB}.counterparty_hive;
RENAME TABLE ${MYSQL_DB}.counterparty to ${MYSQL_DB}.counterparty_last;
RENAME TABLE ${MYSQL_DB}.counterparty_hive to ${MYSQL_DB}.counterparty;
-- DROP TABLE IF EXISTS ${MYSQL_DB}.counterparty_last;
-- -------------

-- -------------
ALTER TABLE ${MYSQL_DB}.counterparty_classify_hive ADD INDEX idx_mcht_cd_data_month (mcht_cd,data_month) USING BTREE ;
ALTER TABLE ${MYSQL_DB}.counterparty_classify_hive ADD UNIQUE index_unique (mcht_cd,data_month,buyer_name,buyer_tax_cd) ;

DROP TABLE IF EXISTS ${MYSQL_DB}.counterparty_classify_last;
CREATE TABLE IF NOT EXISTS ${MYSQL_DB}.counterparty_classify LIKE ${MYSQL_DB}.counterparty_classify_hive;
RENAME TABLE ${MYSQL_DB}.counterparty_classify  to ${MYSQL_DB}.counterparty_classify_last;
RENAME TABLE ${MYSQL_DB}.counterparty_classify_hive to ${MYSQL_DB}.counterparty_classify;
-- DROP TABLE IF EXISTS ${MYSQL_DB}.counterparty_classify_last;
-- -------------

-- -------------
ALTER TABLE ${MYSQL_DB}.statistics_month_hive ADD INDEX idx_mcht_cd_data_month (mcht_cd,data_month) USING BTREE ;
ALTER TABLE ${MYSQL_DB}.statistics_month_hive ADD UNIQUE index_unique (mcht_cd,data_month) ;

DROP TABLE IF EXISTS ${MYSQL_DB}.statistics_month_last;
CREATE TABLE IF NOT EXISTS ${MYSQL_DB}.statistics_month LIKE ${MYSQL_DB}.statistics_month_hive;
RENAME TABLE ${MYSQL_DB}.statistics_month  to ${MYSQL_DB}.statistics_month_last;
RENAME TABLE ${MYSQL_DB}.statistics_month_hive to ${MYSQL_DB}.statistics_month;
-- DROP TABLE IF EXISTS ${MYSQL_DB}.statistics_month_last;
-- -------------

-- -------------
ALTER TABLE ${MYSQL_DB}.statistics_crossmonth_hive ADD INDEX idx_mcht_cd_data_month (mcht_cd,data_month) USING BTREE ;
ALTER TABLE ${MYSQL_DB}.statistics_crossmonth_hive ADD UNIQUE index_unique (mcht_cd,data_month) ;

DROP TABLE IF EXISTS ${MYSQL_DB}.statistics_crossmonth_last;
CREATE TABLE IF NOT EXISTS ${MYSQL_DB}.statistics_crossmonth LIKE ${MYSQL_DB}.statistics_crossmonth_hive;
RENAME TABLE ${MYSQL_DB}.statistics_crossmonth to ${MYSQL_DB}.statistics_crossmonth_last;
RENAME TABLE ${MYSQL_DB}.statistics_crossmonth_hive to ${MYSQL_DB}.statistics_crossmonth;
-- DROP TABLE IF EXISTS ${MYSQL_DB}.statistics_crossmonth_last;
