package dashu

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import util.spark


object saleinvoice_tmp {
  def run(hc:HiveContext): Unit = {
//    val conf:SparkConf = new SparkConf().setAppName("saleinvoice_tmp")
//    val sc:SparkContext = new SparkContext(conf)
//    val hc:HiveContext = new HiveContext(sc)

    val cjlog_tmp_df:DataFrame = hc.sql(spark.printCmd(constant_full.saleinvoice_tmp.cjlog_tmp))
    cjlog_tmp_df.registerTempTable("cjlog_tmp")

    hc.sql(spark.printCmd(constant_full.saleinvoice_tmp.truncate_cjlog_tmp)).show()
    hc.sql(spark.printCmd(constant_full.saleinvoice_tmp.insert_cjlog_tmp)).show()
    println("cjlog_tmp 插入完成")

    val saleinvoice_tmp1_df:DataFrame = hc.sql(spark.printCmd(constant_full.saleinvoice_tmp.saleinvoice_tmp1))
    saleinvoice_tmp1_df.registerTempTable("saleinvoice_tmp1")
    hc.sql(spark.printCmd(constant_full.saleinvoice_tmp.truncate_saleinvoice_tmp1)).show()
    hc.sql(spark.printCmd(constant_full.saleinvoice_tmp.insert_saleinvoice_tmp1)).show()
    println("saleinvoice_tmp1 插入完成")
    
    val saleinvoice_tmp2_df:DataFrame = hc.sql(spark.printCmd(constant_full.saleinvoice_tmp.saleinvoice_tmp2))
    saleinvoice_tmp2_df.registerTempTable("saleinvoice_tmp2")
    hc.sql(spark.printCmd(constant_full.saleinvoice_tmp.truncate_saleinvoice_tmp2)).show()
    hc.sql(spark.printCmd(constant_full.saleinvoice_tmp.insert_saleinvoice_tmp2)).show()
    println("saleinvoice_tmp2 插入完成")

    val saleinvoice_tmp3_df:DataFrame = hc.sql(spark.printCmd(constant_full.saleinvoice_tmp.saleinvoice_tmp3))
    saleinvoice_tmp3_df.registerTempTable("saleinvoice_tmp3")
    hc.sql(spark.printCmd(constant_full.saleinvoice_tmp.truncate_saleinvoice_tmp3)).show()
    hc.sql(spark.printCmd(constant_full.saleinvoice_tmp.insert_saleinvoice_tmp3)).show()
    println("saleinvoice_tmp3 插入完成")

    val saleinvoice_tmp_df:DataFrame = hc.sql(spark.printCmd(constant_full.saleinvoice_tmp.saleinvoice_tmp))
    saleinvoice_tmp_df.registerTempTable("saleinvoice_tmp")
    hc.sql(spark.printCmd(constant_full.saleinvoice_tmp.truncate_saleinvoice_tmp)).show()
    hc.sql(spark.printCmd(constant_full.saleinvoice_tmp.insert_saleinvoice_tmp)).show()
    println("插入saleinvoice_tmp成功")

  }


}
