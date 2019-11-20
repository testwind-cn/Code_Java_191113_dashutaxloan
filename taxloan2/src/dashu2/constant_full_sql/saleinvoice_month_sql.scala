package dashu2.constant_full_sql
import dashu2.constant_common

object saleinvoice_month_sql {

  //salesInvoiceInfo_tmp1_24month 筛选近24个月的数据
  val salesInvoiceInfo_tmp1_24month=
    s"""
      |select
      |t1.sellertaxno, --商户号
      |(case when  length (CAST(MONTH (t1.invoicedate) AS STRING)) =1 then concat(YEAR (t1.invoicedate),"0", MONTH (t1.invoicedate))
      |	      else concat(YEAR (t1.invoicedate),MONTH (t1.invoicedate)) end) AS data_month, --所属时期 月度数
      |sum(CASE WHEN cancelflag='0' THEN totalamount+totaltax ELSE 0 END) AS taxsale_amount,  --应税销售额 --红冲不剔除
      |sum(totalamount+totaltax) AS total_amount,  -- 全部_含税金额合计
      |sum(CASE WHEN invoicetype='s' AND cancelflag='0' THEN totalamount ELSE 0 END) AS taxsale_amount_zp,  --销项增值税专用发票金额 --红冲不剔除
      |sum(CASE WHEN invoicetype='c' AND cancelflag='0' THEN totalamount ELSE 0 END) AS taxsale_amount_pp,  --销项增值税普通发票金额 --红冲不剔除
      |abs(sum(CASE WHEN cancelflag='0' and (totalamount<0 OR totaltax<0) THEN totalamount+totaltax ELSE 0 END)) AS red_amount,  --红票金额  --作废标志为否
      |sum(CASE WHEN cancelflag='1' THEN                                                                                               +totaltax ELSE 0 END) AS invalid_amount, --废票金额
      |COUNT(*) AS valid_num,  --开票数量
      |sum(CASE WHEN invoicetype='s' THEN 1 ELSE 0	END) AS valid_num_zp, --销项增值税专用发票数量
      |sum(CASE WHEN invoicetype='c' THEN 1 ELSE 0	END) AS valid_num_pp,  --销项增值税普通发票数量
      |sum(CASE WHEN cancelflag='0' and (totalamount<0 OR totaltax<0) THEN 1 ELSE 0 END) AS red_num,  --红票数量  --作废标志为否
      |sum(CASE WHEN cancelflag='1' and totalamount+totaltax<>0 THEN 1 ELSE 0 END) AS invalid_num,  --废票数量  --含税金额不等于0
      |sum(CASE WHEN cancelflag='0' THEN totaltax ELSE 0 END) AS tax_amount --交税金额
      |from
      |${constant_common.Const_common.DATABASE}.saleinvoice_tmp t1
      |where t1.buyertaxno<>'' and t1.buyertaxno is not null and t1.buyertaxno not like '00%'  --剔除无效数据
      |and from_unixtime(unix_timestamp(t1.invoicedate,'yyyy-MM-dd'))>= from_unixtime(unix_timestamp('1970-01-01','yyyy-MM-dd')) --日期大于等于1970-01-01
      |and datediff(to_date(from_unixtime(unix_timestamp(t1.invoicedate,'yyyy-MM'))),
      |		to_date(from_unixtime(unix_timestamp( substr(current_date(),1,7),'yyyy-MM'))))<0  --剔除当月数据
      |and to_date(from_unixtime(unix_timestamp(invoicedate,'yyyy-MM-dd')))>=  --取近两年的数据
      |       add_months(date_sub(current_date,dayofmonth(current_date)-1),-24) --24个月前的第一天
      |group by  t1.sellertaxno,(case when  length (CAST(MONTH (t1.invoicedate) AS STRING)) =1 then concat(YEAR (t1.invoicedate),"0", MONTH (t1.invoicedate))
      |	      else concat(YEAR (t1.invoicedate),MONTH (t1.invoicedate)) end)
      |order by t1.sellertaxno,data_month
    """.stripMargin

  //salesInvoiceInfo_tmp1_36month 筛选近36个月的数据
  val salesInvoiceInfo_tmp1_36month=
    s"""
       |select
       |t1.sellertaxno, --商户号
       |(case when  length (CAST(MONTH (t1.invoicedate) AS STRING)) =1 then concat(YEAR (t1.invoicedate),"0", MONTH (t1.invoicedate))
       |	      else concat(YEAR (t1.invoicedate),MONTH (t1.invoicedate)) end) AS data_month, --所属时期 月度数
       |sum(CASE WHEN cancelflag='0' THEN totalamount+totaltax ELSE 0 END) AS taxsale_amount,  --应税销售额 --红冲不剔除
       |sum(totalamount+totaltax) AS total_amount,  -- 全部_含税金额合计
       |sum(CASE WHEN invoicetype='s' AND cancelflag='0' THEN totalamount ELSE 0 END) AS taxsale_amount_zp,  --销项增值税专用发票金额 --红冲不剔除
       |sum(CASE WHEN invoicetype='c' AND cancelflag='0' THEN totalamount ELSE 0 END) AS taxsale_amount_pp,  --销项增值税普通发票金额 --红冲不剔除
       |abs(sum(CASE WHEN cancelflag='0' and (totalamount<0 OR totaltax<0) THEN totalamount+totaltax ELSE 0 END)) AS red_amount,  --红票金额  --作废标志为否
       |sum(CASE WHEN cancelflag='1' THEN totalamount+totaltax ELSE 0 END) AS invalid_amount, --废票金额
       |COUNT(*) AS valid_num,  --开票数量
       |sum(CASE WHEN invoicetype='s' THEN 1 ELSE 0	END) AS valid_num_zp, --销项增值税专用发票数量
       |sum(CASE WHEN invoicetype='c' THEN 1 ELSE 0	END) AS valid_num_pp,  --销项增值税普通发票数量
       |sum(CASE WHEN cancelflag='0' and (totalamount<0 OR totaltax<0) THEN 1 ELSE 0 END) AS red_num,  --红票数量  --作废标志为否
       |sum(CASE WHEN cancelflag='1' and totalamount+totaltax<>0 THEN 1 ELSE 0 END) AS invalid_num,  --废票数量  --含税金额不等于0
       |sum(CASE WHEN cancelflag='0' THEN totaltax ELSE 0 END) AS tax_amount --交税金额
       |from
       |${constant_common.Const_common.DATABASE}.saleinvoice_tmp t1
       |where t1.buyertaxno<>'' and t1.buyertaxno is not null and t1.buyertaxno not like '00%'  --剔除无效数据
       |and from_unixtime(unix_timestamp(t1.invoicedate,'yyyy-MM-dd'))>= from_unixtime(unix_timestamp('1970-01-01','yyyy-MM-dd')) --日期大于等于1970-01-01
       |and datediff(to_date(from_unixtime(unix_timestamp(t1.invoicedate,'yyyy-MM'))),
       |		to_date(from_unixtime(unix_timestamp( substr(current_date(),1,7),'yyyy-MM'))))<0  --剔除当月数据
       |and to_date(from_unixtime(unix_timestamp(invoicedate,'yyyy-MM-dd')))>=  --取近两年的数据
       |       add_months(date_sub(current_date,dayofmonth(current_date)-1),-36) --36个月前的第一天
       |group by  t1.sellertaxno,(case when  length (CAST(MONTH (t1.invoicedate) AS STRING)) =1 then concat(YEAR (t1.invoicedate),"0", MONTH (t1.invoicedate))
       |	      else concat(YEAR (t1.invoicedate),MONTH (t1.invoicedate)) end)
       |order by t1.sellertaxno,data_month
     """.stripMargin

  //生成sellertaxno_24month商户最近36month维度表
  val dim_sellertaxno_36month=
    s"""
      |select
      |	t1.sellertaxno,
      |	d.month
      |from ${constant_common.Const_common.DATABASE}.dim_date d cross join
      |(
      |select
      |	distinct sellertaxno
      |from ${constant_common.Const_common.DATABASE}.salesInvoiceInfo_tmp1_24month
      |) t1
      |where datediff(to_date(from_unixtime(unix_timestamp(d.month,'yyyyMM'))),
      |		to_date(from_unixtime(unix_timestamp( substr(add_months(current_date(),-36),1,7),'yyyy-MM'))))>=0
      |and
      |datediff(to_date(from_unixtime(unix_timestamp(d.month,'yyyyMM'))),
      |		to_date(from_unixtime(unix_timestamp( substr(current_date(),1,7),'yyyy-MM'))))< 0 --取近36个月的维度
      |order by sellertaxno,month
    """.stripMargin

  //关联36个月的维度表为计算同比、环比等指标
  val salesInvoiceInfo_tmp1_36month_dim=
    s"""
      |select
      |dim.sellertaxno,
      |dim.month,
      |nvl(tmp1.taxsale_amount,0) as taxsale_amount
      |from ${constant_common.Const_common.DATABASE}.dim_sellertaxno_36month dim
      |left join salesInvoiceInfo_tmp1_36month tmp1
      |on dim.sellertaxno=tmp1.sellertaxno and dim.month=tmp1.data_month
      |order by sellertaxno,month
    """.stripMargin

  //--关联维度表并计算除同比环比以外的所有其它近24个月的指标
  val salesInvoiceInfo_tmp1_24month_dim=
    s"""
      |select
      |dim.sellertaxno,
      |dim.month,
      |nvl(tmp1.taxsale_amount,0) as taxsale_amount,  --应税销售额
      |nvl(tmp1.total_amount,0) as total_amount, --全部_含税金额合计
      |nvl(tmp1.taxsale_amount_zp,0) as taxsale_amount_zp, --销项增值税专用发票金额
      |nvl(tmp1.taxsale_amount_pp,0) as taxsale_amount_pp, --销项增值税普通发票金额
      |nvl(tmp1.tax_amount,0) as tax_amount,  --交税金额
      |nvl(tmp1.red_amount,0) as red_amount,  --红票金额
      |nvl(tmp1.invalid_amount,0) as invalid_amount,  --废票金额
      |nvl(tmp1.valid_num,0) as valid_num,  --开票数量
      |nvl(tmp1.valid_num_zp,0) as valid_num_zp,  --销项增值税专用发票数量
      |nvl(tmp1.valid_num_pp,0) as valid_num_pp,  --销项增值税普通发票数量
      |nvl(tmp1.red_num,0) as red_num,  --红票数量
      |nvl(tmp1.invalid_num,0) as invalid_num,  --废票数量
      |
      |(case when abs(nvl(tmp1.taxsale_amount,0)+ nvl(tmp1.red_amount,0)) <=0.001 then -99999
      |		  else round(nvl(tmp1.red_amount,0) / (nvl(tmp1.taxsale_amount,0)+ nvl(tmp1.red_amount,0)),4)*100
      |		  end) as red_amount_ratio, --红票金额占比
      |(case when nvl(tmp1.total_amount,0) <=0.001 then -99999
      |		  else round(nvl(tmp1.invalid_amount,0) / nvl(tmp1.total_amount,0),4)*100
      |		  end) as invalid_amount_ratio, --废票金额占比
      |
      |(case when abs(nvl(tmp1.valid_num,0)-nvl(tmp1.invalid_num,0)) <=0.001 then -99999
      |		  else round(nvl(tmp1.red_num,0) / (nvl(tmp1.valid_num,0)-nvl(tmp1.invalid_num,0)),4)*100
      |		  end) as red_num_ratio, --红票数量占比
      |(case when abs(nvl(tmp1.valid_num,0)) <=0.001 then -99999
      |		  else round(nvl(tmp1.invalid_amount,0) / nvl(tmp1.valid_num,0),4)*100
      |		  end) as invalid_num_ratio --废票数量占比
      |
      |from ${constant_common.Const_common.DATABASE}.dim_sellertaxno_24month dim
      |left join ${constant_common.Const_common.DATABASE}.salesInvoiceInfo_tmp1_24month tmp1
      |on dim.sellertaxno=tmp1.sellertaxno and dim.month=tmp1.data_month
      |order by sellertaxno,month
    """.stripMargin

  //利用分析函数计算每个月前一个月和前一年的应税销售额
  val salesInvoiceInfo_tmp1_36month_dim_lag=
    s"""
      |select
      |sellertaxno,
      |month,
      |taxsale_amount,
      |lag(taxsale_amount,1,0) over(partition by sellertaxno
      |			order by to_date(from_unixtime(unix_timestamp(month,'yyyyMM')))) as taxsale_amount_1_month_ago,
      |lag(taxsale_amount,12,0) over(partition by sellertaxno
      |			order by to_date(from_unixtime(unix_timestamp(month,'yyyyMM')))) as taxsale_amount_1_year_ago
      |from ${constant_common.Const_common.DATABASE}.salesInvoiceInfo_tmp1_36month_dim
    """.stripMargin

  //--生成最终结果表saleinvoice_month
  val saleinvoice_month=
    s"""
      |select
      |dim.sellertaxno as taxno,
      |dim.month,
      |dim.taxsale_amount,  --应税销售额
      |dim.taxsale_amount_zp, --销项增值税专用发票金额
      |dim.taxsale_amount_pp, --销项增值税普通发票金额
      |dim.tax_amount,  --交税金额
      |dim.red_amount,  --红票金额
      |dim.invalid_amount,  --废票金额
      |dim.valid_num,  --开票数量
      |dim.valid_num_zp,  --销项增值税专用发票数量
      |dim.valid_num_pp,  --销项增值税普通发票数量
      |dim.red_num,  --红票数量
      |dim.invalid_num,  --废票数量
      |dim.red_amount_ratio as red_amount_ratio, --红票金额占比
      |dim.invalid_amount_ratio as invalid_amount_ratio, --废票金额占比
      |dim.red_num_ratio as red_num_ratio, --红票数量占比
      |dim.invalid_num_ratio as invalid_num_ratio, --废票数量占比
      |
      |(case when abs(lag.taxsale_amount_1_year_ago) <=0.001 or lag.taxsale_amount_1_year_ago is null then -99999
      |		  else round((dim.taxsale_amount / lag.taxsale_amount_1_year_ago)-1,4)*100
      |		  end) as taxsale_amount_yoy, --应税销售额同比
      |
      |(case when abs(lag.taxsale_amount_1_month_ago) <=0.001 or lag.taxsale_amount_1_month_ago is null then -99999
      |		  else round((dim.taxsale_amount / lag.taxsale_amount_1_month_ago)-1,4)*100
      |		  end) as taxsale_amount_mom, --应税销售额环比
      |    current_user() as create_user,
      |    current_date() as modify_time,
      |    current_user() as modify_user,
      |    current_date() as create_time
      |from ${constant_common.Const_common.DATABASE}.salesInvoiceInfo_tmp1_24month_dim dim
      |left join ${constant_common.Const_common.DATABASE}.salesInvoiceInfo_tmp1_36month_dim_lag lag
      |on dim.sellertaxno=lag.sellertaxno and dim.month=lag.month
      |order by taxno,dim.month
    """.stripMargin

  val insertSql =
    s"""
      |INSERT INTO TABLE ${constant_common.Const_common.DATABASE}.saleinvoice_month
      |PARTITION(create_time)
      |SELECT
      |taxno,
      |month,
      |taxsale_amount,
      |taxsale_amount_zp,
      |taxsale_amount_pp,
      |tax_amount,
      |red_amount,
      |invalid_amount,
      |valid_num,
      |valid_num_zp,
      |valid_num_pp,
      |red_num,
      |invalid_num,
      |red_amount_ratio,
      |invalid_amount_ratio,
      |red_num_ratio,
      |invalid_num_ratio,
      |taxsale_amount_yoy,
      |taxsale_amount_mom,
      |create_user,
      |modify_time,
      |modify_user,
      |create_time
      |FROM tmp_saleinvoice_month
    """.stripMargin

}
