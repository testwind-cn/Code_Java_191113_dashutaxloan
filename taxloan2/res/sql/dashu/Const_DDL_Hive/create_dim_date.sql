-- CREATE TABLE IF NOT EXISTS  dim_db.dim_date
-- (
--     date_sk    int COMMENT 'surrogate key, 主键',
--     date       date COMMENT 'date,yyyy-mm-dd',
--     date_name  string COMMENT 'date,yyyy年mm月dd日，中文日期',
--     dayofweek  string COMMENT 'day of week，星期',
--     month      tinyint COMMENT 'month,月',
--     month_name varchar(9) COMMENT 'month name,月名称',
--     quarter    tinyint COMMENT 'quarter,季度',
--     year       smallint COMMENT 'year,年'
-- ) COMMENT 'date dimension table,通用日期日历表'
-- ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
-- WITH SERDEPROPERTIES ( 'field.delim' = ',', 'serialization.format' = ',')
-- STORED AS TEXTFILE
CREATE TABLE IF NOT EXISTS dim_db.dim_date
ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES
    ( 'field.delim' = ',', 'serialization.format' = ',')
STORED AS TEXTFILE
as
select
    row_id as date_sk
    ,date_add( '2000-01-01', row_id-1 ) as date_value
--    ,from_unixtime( unix_timestamp(date_add( '2000-01-01', row_id-1 ),'yyyy-MM-dd'), 'yyyy-MM-dd')  as date_value     -- 'yyyy-MM-dd'
    ,from_unixtime( unix_timestamp(date_add( '2000-01-01', row_id-1 ),'yyyy-MM-dd'), 'yyyy年MM月dd日') as date_name -- 'yyyy年MM月dd日'
    ,tinyint((row_id+4) % 7 + 1)  as dateofweek           --  星期一 1， 星期二 2， 星期六 6，星期日 7
    ,tinyint(month( date_add( '2000-01-01', row_id-1 ) )) as month_value -- 月 1,2
    ,t_month.m2 as month_name  -- ,'January'
    ,tinyint((month( date_add( '2000-01-01', row_id-1 ) ) - 1 ) / 3 + 1) as quarter_value -- 月 1,2,3
    ,smallint(year(date_add( '2000-01-01', row_id-1 ))) as year_value      -- 年
from
(
    select
        ( row_number() over() )  as row_id
    from
    (
        select explode( split( substring(repeat(',0',11323),2) ,',' ) ) as rowdata
    ) rtable
) date_table
left join
(
    select row_month[0] as m1, row_month[1] as m2
    from
    (
        select
            explode(
                array(
                    array(1,'January'),
                    array(2,'February'),
                    array(3,'March'),
                    array(4,'April'),
                    array(5,'May'),
                    array(6,'June'),
                    array(7,'July'),
                    array(8,'August'),
                    array(9,'September'),
                    array(10,'October'),
                    array(11,'November'),
                    array(12,'December')
                )
            ) as row_month
    ) t1
) t_month
on
    month( date_add( '2000-01-01', row_id-1 ) )=t_month.m1
order by date_sk