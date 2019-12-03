-- CREATE TABLE ${hivevar:DATABASE_DEST}.dim_month
-- (
--  `month` string,
--  `year` smallint)
-- ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
-- STORED AS TEXTFILE
CREATE TABLE IF NOT EXISTS ${hivevar:DATABASE_DEST}.dim_month
ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'escapeChar'='\\',
    'quoteChar'='\"',
    'separatorChar'=','
)
STORED AS TEXTFILE
as
select
    cast( ( (2000+ int(row_id / 12))*100 + int(row_id % 12) +1  ) as string )  as  month,
    cast(   (2000+ int(row_id / 12)) as string ) as year
from
(
    select
        ( ( row_number() over() ) - 1 )  as row_id
    from
    (
        select explode( split( substring(repeat(',0',600),2) ,',' ) ) as rowdata
    ) rtable
) row_table
order by month