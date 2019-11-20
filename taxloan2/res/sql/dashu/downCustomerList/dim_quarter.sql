select distinct concat(year,0,quarter) as quarter from allinpal_dw.dim_date d
where datediff(to_date(from_unixtime(unix_timestamp(substr(d.date,1,7),'yyyy-MM'))),
               to_date(from_unixtime(unix_timestamp( substr(add_months(current_date(),-24),1,7),'yyyy-MM'))))>=0
  and
        datediff(to_date(from_unixtime(unix_timestamp(substr(d.date,1,7),'yyyy-MM'))),
                 to_date(from_unixtime(unix_timestamp( substr(current_date(),1,7),'yyyy-MM'))))< 0 --取近24个月的维度
