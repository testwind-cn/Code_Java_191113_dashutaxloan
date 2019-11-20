package dashu2.constant_full_sql
import dashu2.constant_common

object saleinvoice_tmp_sql {
  val cjlog_tmp =
    s"""
      |with
      |cjlog_in as
      |(
      |    select
      |        taxno,
      |        ( CASE
      |              WHEN oldtaxno='null' or oldtaxno='' THEN ''
      |              ELSE oldtaxno
      |            END
      |        ) AS oldtaxno
      |        ,mflag,finish_time
      |    from ${constant_common.Const_common.DATABASE}.cjlog
      |),
      |new_taxno_t as
      |(
      |    select
      |        COALESCE(taxno,'') as taxno,
      |        CASE WHEN locate('.', oldtaxno) > 0
      |            THEN SUBSTRING(oldtaxno,1,locate('.', oldtaxno)-1)
      |            ELSE COALESCE(oldtaxno,'')
      |        END as oldtaxno
      |        , max(finish_time) last_time
      |        , min(finish_time) first_time
      |    from cjlog_in
      |    where
      |        mflag = 1 and finish_time is not null and
      |        (
      |            finish_time >= '2011-11-01 00:00:00.0'
      |        )
      |    group by
      |        taxno,
      |        CASE WHEN locate('.', oldtaxno) > 0
      |            THEN SUBSTRING(oldtaxno,1,locate('.', oldtaxno)-1)
      |        ELSE COALESCE(oldtaxno,'')
      |        END
      |),
      |all_taxno_t as
      |(
      |    select
      |        COALESCE(taxno,'') as taxno,
      |        CASE WHEN locate('.', oldtaxno) > 0
      |            THEN SUBSTRING(oldtaxno,1,locate('.', oldtaxno)-1)
      |            ELSE COALESCE(oldtaxno,'')
      |        END as oldtaxno
      |        , max(finish_time) last_time
      |        , min(finish_time) first_time
      |    from cjlog_in
      |    where
      |        mflag = 1 and finish_time is not null
      |    group by
      |        taxno,
      |        CASE WHEN locate('.', oldtaxno) > 0
      |            THEN SUBSTRING(oldtaxno,1,locate('.', oldtaxno)-1)
      |        ELSE COALESCE(oldtaxno,'')
      |        END
      |)
      |select
      |    distinct
      |    coalesce( all_taxno_t.taxno,new_taxno_t.taxno) as taxno,
      |    coalesce( all_taxno_t.oldtaxno,new_taxno_t.oldtaxno) as oldtaxno
      |--     CASE WHEN  coalesce( all_taxno_t.last_time,new_taxno_t.last_time) > new_taxno_t.last_time
      |--         THEN coalesce( all_taxno_t.last_time,new_taxno_t.last_time)
      |--         ELSE new_taxno_t.last_time
      |--     END as last_time
      |from
      |    new_taxno_t
      |left join
      |    all_taxno_t
      |on new_taxno_t.taxno = all_taxno_t.oldtaxno
    """.stripMargin

  val truncate_cjlog_tmp =
    s"""
       |truncate table ${constant_common.Const_common.DATABASE}.cjlog_tmp
    """.stripMargin

  val insert_cjlog_tmp =
    s"""
       |insert into ${constant_common.Const_common.DATABASE}.cjlog_tmp select * from cjlog_tmp
    """.stripMargin

  val truncate_saleinvoice_tmp =
    s"""
       |truncate table ${constant_common.Const_common.DATABASE}.saleinvoice_tmp
    """.stripMargin

  val insert_saleinvoice_tmp =
    s"""
       |insert into ${constant_common.Const_common.DATABASE}.saleinvoice_tmp select * from saleinvoice_tmp
    """.stripMargin

  val truncate_saleinvoice_tmp1 =
    s"""
       |truncate table ${constant_common.Const_common.DATABASE}.saleinvoice_tmp1
    """.stripMargin

  val insert_saleinvoice_tmp1 =
    s"""
       |insert into ${constant_common.Const_common.DATABASE}.saleinvoice_tmp1 select * from saleinvoice_tmp1
    """.stripMargin

  val truncate_saleinvoice_tmp2 =
    s"""
       |truncate table ${constant_common.Const_common.DATABASE}.saleinvoice_tmp2
    """.stripMargin

  val insert_saleinvoice_tmp2 =
    s"""
       |insert into ${constant_common.Const_common.DATABASE}.saleinvoice_tmp2 select * from saleinvoice_tmp2
    """.stripMargin

  val truncate_saleinvoice_tmp3 =
    s"""
       |truncate table ${constant_common.Const_common.DATABASE}.saleinvoice_tmp3
    """.stripMargin

  val insert_saleinvoice_tmp3 =
    s"""
       |insert into ${constant_common.Const_common.DATABASE}.saleinvoice_tmp3 select * from saleinvoice_tmp3
    """.stripMargin


  //处理oldtaxno为空或者null的记录
  val saleinvoice_tmp1 =
    s"""
       |select
       |		c1.taxno AS sellertaxno,
       |		s1.sellername,
       |		s1.selleraddtel,
       |		s1.invoicedate,
       |		(case when length (CAST(MONTH (s1.invoicedate) AS STRING)) =1 then concat(YEAR (s1.invoicedate),"0", MONTH (s1.invoicedate))
       |	      else concat(YEAR (s1.invoicedate),MONTH (s1.invoicedate)) end) AS data_month,
       |		s1.sellerbankno,
       |		c1.oldtaxno,
       |		s1.invoiceid,
       |		s1.buyername,
       |		s1.buyertaxno,
       |		s1.totalamount,
       |		s1.totaltax,
       |		s1.cancelflag,
       |		s1.invoicetype,
       |		s1.jztype
       |from ${constant_common.Const_common.DATABASE}.cjlog_tmp c1
       |left join ${constant_common.Const_common.DATABASE}.saleinvoice s1
       |on c1.taxno=s1.sellertaxno
       |where c1.oldtaxno='null' or c1.oldtaxno=''
    """.stripMargin

  //处理oldtaxno!='null'，且oldtaxno!=''的发票数据，合并新老税号数据
  val saleinvoice_tmp2 =
    s"""
       |select
       |	s3.taxno AS sellertaxno,
       |	s3.sellername,
       |	s3.selleraddtel,
       |	s3.invoicedate,
       |	(case when  length (CAST(MONTH (s3.invoicedate) AS STRING)) =1 then concat(YEAR (s3.invoicedate),"0", MONTH (s3.invoicedate))
       |	      else concat(YEAR (s3.invoicedate),MONTH (s3.invoicedate)) end) AS data_month,
       |	s3.sellerbankno,
       |	s3.oldtaxno,
       |	s3.invoiceid,
       |	s3.buyername,
       |	s3.buyertaxno,
       |	s3.totalamount,
       |	s3.totaltax,
       |	s3.cancelflag,
       |	s3.invoicetype,
       |	s3.jztype
       |from
       |(
       |select
       |		c1.taxno,
       |		s1.sellertaxno,
       |		s1.sellername,
       |		s1.selleraddtel,
       |		s1.invoicedate,
       |		s1.sellerbankno,
       |		c1.oldtaxno,
       |		s1.invoiceid,
       |		s1.buyername,
       |		s1.buyertaxno,
       |		s1.totalamount,
       |		s1.totaltax,
       |		s1.cancelflag,
       |		s1.invoicetype,
       |		s1.jztype
       |
      |from ${constant_common.Const_common.DATABASE}.cjlog_tmp c1
       |left join ${constant_common.Const_common.DATABASE}.saleinvoice s1
       |on c1.taxno=s1.sellertaxno
       |where c1.oldtaxno!='null' and c1.oldtaxno!=''
       |
      |union all
       |
      |select
       |		c2.taxno,
       |		s2.sellertaxno,
       |		s2.sellername,
       |		s2.selleraddtel,
       |		s2.invoicedate,
       |		s2.sellerbankno,
       |		c2.oldtaxno,
       |		s2.invoiceid,
       |		s2.buyername,
       |		s2.buyertaxno,
       |		s2.totalamount,
       |		s2.totaltax,
       |		s2.cancelflag,
       |		s2.invoicetype,
       |		s2.jztype
       |
      |from ${constant_common.Const_common.DATABASE}.cjlog_tmp c2
       |left join ${constant_common.Const_common.DATABASE}.saleinvoice s2
       |on c2.oldtaxno=s2.sellertaxno
       |where c2.oldtaxno!='null' and c2.oldtaxno!=''
       |)s3
    """.stripMargin


  val saleinvoice_tmp3 =
    s"""
       |select * from ${constant_common.Const_common.DATABASE}.saleinvoice_tmp1
       |union all
       |select * from ${constant_common.Const_common.DATABASE}.saleinvoice_tmp2
    """.stripMargin

  //过滤重复的发票数据
  val saleinvoice_tmp =
    s"""
       |select
       |sellertaxno,sellername,selleraddtel,invoicedate,data_month,sellerbankno,
       |oldtaxno,invoiceid,buyername,buyertaxno,totalamount,totaltax,cancelflag,invoicetype,jztype
       |
      |from (
       |select sellertaxno,sellername,selleraddtel,sellerbankno,invoicedate,data_month,
       |oldtaxno,invoiceid,buyername,buyertaxno,totalamount,totaltax,cancelflag,invoicetype,jztype,
       |row_number() over(partition by invoiceid order by invoicedate) as rank
       |from ${constant_common.Const_common.DATABASE}.saleinvoice_tmp3
       |)a where a.rank=1 and a.invoiceid!='NULL'
    """.stripMargin
}
