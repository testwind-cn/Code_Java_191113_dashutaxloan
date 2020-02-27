select distinct concat(year_value,'0',quarter_value) as quarter from dim_db.dim_date d
where datediff(to_date(from_unixtime(unix_timestamp(substr(d.date_value,1,7),'yyyy-MM'))),
               to_date(from_unixtime(unix_timestamp( substr(add_months(current_date(),-24),1,7),'yyyy-MM'))))>=0
  and
        datediff(to_date(from_unixtime(unix_timestamp(substr(d.date_value,1,7),'yyyy-MM'))),
                 to_date(from_unixtime(unix_timestamp( substr(current_date(),1,7),'yyyy-MM'))))< 0 --取近24个月的维度
