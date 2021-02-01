package com.zhy.streamx.binlog.reader

import com.zhy.streamx.core.api.reader.AbstractReader
import com.zhy.streamx.core.entity.ConfigConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2021-02-01
  *  \* Time: 15:45
  *  \* Description: 
  *  \*/
class BinlogReader(ssc: StreamingContext, spark: SparkSession, configConf: ConfigConf)
        extends AbstractReader(ssc, spark, configConf) {
    /**
      * 初始化
      *
      * @param cName
      * @param readerConfigStr
      */
    override def init(cName: String, readerConfigStr: String): Unit = {

    }

    /**
      * 读取数据
      */
    override def read(): Unit = {
        val df = spark.readStream.
                format("org.apache.spark.sql.mlsql.sources.MLSQLBinLogDataSource").
                option("host","10.57.30.217").
                option("port","3306").
                option("userName","root").
                option("password","123456").
                option("databaseNamePattern","zhy").
                option("tableNamePattern","zb").
                load()
//        df.foreachPartition()
    }

    /**
      * 表示reader唯一性主键
      *
      * @return
      */
    override def primaryKey: String = ???

    /**
      * 转临时表名
      *
      * @return
      */
    override def tblTable: String = ???
}
