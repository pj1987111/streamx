package com.zhy.streamx.stdout.writer

import com.zhy.streamx.core.api.writer.AbstractWriter
import com.zhy.streamx.core.entity.ConfigConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2021-01-26
  *  \* Time: 18:50
  *  \* Description: 
  *  \*/
class StdoutWriter(spark: SparkSession, configConf: ConfigConf) extends AbstractWriter(spark, configConf) {

    override def init(cName: String, writerConfigStr: String): Unit = {
        this.cName = cName
    }

    override def write(df: DataFrame): Unit = {
        createView(df)
        val dfT = spark.sql(selectTmpSql())
        dfT.show(20)
//        val datas = dfT.collect()
//        println("data size is : "+datas.size)
//        val schemaList = dfT.schema.toList
//        dfT.foreachPartition(iterator => {
//            iterator.foreach(row => {
//                println(row)
//                logger.info(row.toString())
//            })
//        })
    }
}
