package dashu2

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

object saleinvoice_tmp {
  def main(args: Array[String]): Unit = {
    val conf:SparkConf = new SparkConf().setAppName("saleinvoice_tmp")
    val sc:SparkContext = new SparkContext(conf)
    val hc:HiveContext = new HiveContext(sc)

    val cjlog_tmp_df:DataFrame = hc.sql(constant_full_sql.saleinvoice_tmp_sql.cjlog_tmp)
    cjlog_tmp_df.registerTempTable("cjlog_tmp")

    hc.sql(constant_full_sql.saleinvoice_tmp_sql.truncate_cjlog_tmp)
    hc.sql(constant_full_sql.saleinvoice_tmp_sql.insert_cjlog_tmp)
    println("cjlog_tmp 插入完成")

    val saleinvoice_tmp1_df:DataFrame = hc.sql(constant_full_sql.saleinvoice_tmp_sql.saleinvoice_tmp1)
    saleinvoice_tmp1_df.registerTempTable("saleinvoice_tmp1")
    hc.sql(constant_full_sql.saleinvoice_tmp_sql.truncate_saleinvoice_tmp1)
    hc.sql(constant_full_sql.saleinvoice_tmp_sql.insert_saleinvoice_tmp1)
    println("saleinvoice_tmp1 插入完成")
    
    val saleinvoice_tmp2_df:DataFrame = hc.sql(constant_full_sql.saleinvoice_tmp_sql.saleinvoice_tmp2)
    saleinvoice_tmp2_df.registerTempTable("saleinvoice_tmp2")
    hc.sql(constant_full_sql.saleinvoice_tmp_sql.truncate_saleinvoice_tmp2)
    hc.sql(constant_full_sql.saleinvoice_tmp_sql.insert_saleinvoice_tmp2)
    println("saleinvoice_tmp2 插入完成")

    val saleinvoice_tmp3_df:DataFrame = hc.sql(constant_full_sql.saleinvoice_tmp_sql.saleinvoice_tmp3)
    saleinvoice_tmp3_df.registerTempTable("saleinvoice_tmp3")
    hc.sql(constant_full_sql.saleinvoice_tmp_sql.truncate_saleinvoice_tmp3)
    hc.sql(constant_full_sql.saleinvoice_tmp_sql.insert_saleinvoice_tmp3)
    println("saleinvoice_tmp3 插入完成")

    val saleinvoice_tmp_df:DataFrame = hc.sql(constant_full_sql.saleinvoice_tmp_sql.saleinvoice_tmp)
    saleinvoice_tmp_df.registerTempTable("saleinvoice_tmp")
    hc.sql(constant_full_sql.saleinvoice_tmp_sql.truncate_saleinvoice_tmp)
    hc.sql(constant_full_sql.saleinvoice_tmp_sql.insert_saleinvoice_tmp)
    println("插入saleinvoice_tmp成功")

  }


}
