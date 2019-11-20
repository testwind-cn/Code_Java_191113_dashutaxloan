-- 生成最终结果表saleinvoice_month
select
    dim.sellertaxno as taxno,
    dim.month,
    dim.taxsale_amount,  --应税销售额
    dim.taxsale_amount_zp, --销项增值税专用发票金额
    dim.taxsale_amount_pp, --销项增值税普通发票金额
    dim.tax_amount,  --交税金额
    dim.red_amount,  --红票金额
    dim.invalid_amount,  --废票金额
    dim.valid_num,  --开票数量
    dim.valid_num_zp,  --销项增值税专用发票数量
    dim.valid_num_pp,  --销项增值税普通发票数量
    dim.red_num,  --红票数量
    dim.invalid_num,  --废票数量
    dim.red_amount_ratio as red_amount_ratio, --红票金额占比
    dim.invalid_amount_ratio as invalid_amount_ratio, --废票金额占比
    dim.red_num_ratio as red_num_ratio, --红票数量占比
    dim.invalid_num_ratio as invalid_num_ratio, --废票数量占比

    (case when abs(lag.taxsale_amount_1_year_ago) <=0.001 or lag.taxsale_amount_1_year_ago is null then -99999
          else round((dim.taxsale_amount / lag.taxsale_amount_1_year_ago)-1,4)*100
        end) as taxsale_amount_yoy, --应税销售额同比

    (case when abs(lag.taxsale_amount_1_month_ago) <=0.001 or lag.taxsale_amount_1_month_ago is null then -99999
          else round((dim.taxsale_amount / lag.taxsale_amount_1_month_ago)-1,4)*100
        end) as taxsale_amount_mom, --应税销售额环比
    current_user() as create_user,
    current_date() as modify_time,
    current_user() as modify_user,
    current_date() as create_time
from ${hivevar:DATABASE_DEST}.salesInvoiceInfo_tmp1_24month_dim dim
         left join ${hivevar:DATABASE_DEST}.salesInvoiceInfo_tmp1_36month_dim_lag lag
                   on dim.sellertaxno=lag.sellertaxno and dim.month=lag.month
order by taxno,dim.month
