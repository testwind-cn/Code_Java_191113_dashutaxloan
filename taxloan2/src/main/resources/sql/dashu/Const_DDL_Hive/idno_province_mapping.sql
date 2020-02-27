CREATE TABLE IF NOT EXISTS dim_db.idno_province_mapping
ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES
    ( 'field.delim' = ',', 'serialization.format' = ',')
STORED AS TEXTFILE
as
select
    row_data[0] as provinceid, row_data[1] as provincename
from
(
    select
        explode(
            array(
                array('11','北京市'),
                array('12','天津市'),
                array('13','河北省'),
                array('14','山西省'),
                array('15','内蒙古自治区'),
                array('21','辽宁省'),
                array('22','吉林省'),
                array('23','黑龙江省'),
                array('31','上海市'),
                array('32','江苏省'),
                array('33','浙江省'),
                array('34','安徽省'),
                array('35','福建省'),
                array('36','江西省'),
                array('37','山东省'),
                array('41','河南省'),
                array('42','湖北省'),
                array('43','湖南省'),
                array('44','广东省'),
                array('45','广西壮族自治区'),
                array('46','海南省'),
                array('50','重庆市'),
                array('51','四川省'),
                array('52','贵州省'),
                array('53','云南省'),
                array('54','西藏自治区'),
                array('61','陕西省'),
                array('62','甘肃省'),
                array('63','青海省'),
                array('64','宁夏回族自治区'),
                array('65','新疆维吾尔自治区'),
                array('71','台湾地区(886)'),
                array('81','香港特别行政区（852)'),
                array('82','澳门特别行政区（853)')
            )
        ) as row_data
) t_data