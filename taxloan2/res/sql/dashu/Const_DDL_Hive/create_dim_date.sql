CREATE TABLE IF NOT EXISTS  dim.dim_date
(
    date_sk    int COMMENT 'surrogate key, 主键',
    date       date COMMENT 'date,yyyy-mm-dd',
    date_name  string COMMENT 'date,yyyy年mm月dd日，中文日期',
    dayofweek  string COMMENT 'day of week，星期',
    month      tinyint COMMENT 'month,月',
    month_name varchar(9) COMMENT 'month name,月名称',
    quarter    tinyint COMMENT 'quarter,季度',
    year       smallint COMMENT 'year,年'
) COMMENT 'date dimension table,通用日期日历表'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ( 'field.delim' = ',', 'serialization.format' = ',')
STORED AS TEXTFILE