-- 关联维度表并计算除同比环比以外的所有其它近24个月的指标
select
    dim.sellertaxno,
    dim.month,
    nvl(tmp1.taxsale_amount,0) as taxsale_amount,  --应税销售额
    nvl(tmp1.total_amount,0) as total_amount, --全部_含税金额合计
    nvl(tmp1.taxsale_amount_zp,0) as taxsale_amount_zp, --销项增值税专用发票金额
    nvl(tmp1.taxsale_amount_pp,0) as taxsale_amount_pp, --销项增值税普通发票金额
    nvl(tmp1.tax_amount,0) as tax_amount,  --交税金额
    nvl(tmp1.red_amount,0) as red_amount,  --红票金额
    nvl(tmp1.invalid_amount,0) as invalid_amount,  --废票金额
    nvl(tmp1.valid_num,0) as valid_num,  --开票数量
    nvl(tmp1.valid_num_zp,0) as valid_num_zp,  --销项增值税专用发票数量
    nvl(tmp1.valid_num_pp,0) as valid_num_pp,  --销项增值税普通发票数量
    nvl(tmp1.red_num,0) as red_num,  --红票数量
    nvl(tmp1.invalid_num,0) as invalid_num,  --废票数量
    (case when abs(nvl(tmp1.taxsale_amount,0)+ nvl(tmp1.red_amount,0)) <=0.5 then -99999
          else round(nvl(tmp1.red_amount,0) / (nvl(tmp1.taxsale_amount,0)+ nvl(tmp1.red_amount,0)),4)*100
        end) as red_amount_ratio, --红票金额占比
    (case when nvl(tmp1.total_amount,0) <=0.5 then -99999
          else round(nvl(tmp1.invalid_amount,0) / nvl(tmp1.total_amount,0),4)*100
        end) as invalid_amount_ratio, --废票金额占比
    (case when abs(nvl(tmp1.valid_num,0)-nvl(tmp1.invalid_num,0)) <=0.02 then -99999
          else round(nvl(tmp1.red_num,0) / (nvl(tmp1.valid_num,0)-nvl(tmp1.invalid_num,0)),4)*100
        end) as red_num_ratio, --红票数量占比
    (case when abs(nvl(tmp1.valid_num,0)) <=0.02 then -99999
          else round(nvl(tmp1.invalid_num,0) / nvl(tmp1.valid_num,0),4)*100
        end) as invalid_num_ratio --废票数量占比
from ${hivevar:DATABASE_DEST}.dim_sellertaxno_24month dim
         left join ${hivevar:DATABASE_DEST}.salesInvoiceInfo_tmp1_24month tmp1
                   on dim.sellertaxno=tmp1.sellertaxno and dim.month=tmp1.data_month
order by sellertaxno,month