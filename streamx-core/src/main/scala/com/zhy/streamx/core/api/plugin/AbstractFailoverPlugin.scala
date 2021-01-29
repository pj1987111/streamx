package com.zhy.streamx.core.api.plugin

import com.zhy.streamx.core.entity.StreamxConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-06-07
  *  \* Time: 20:47
  *  \* Description: 异常文件恢复通用接口，可以对接不同文件系统
  *  \*/
abstract class AbstractFailoverPlugin(ssc: StreamingContext, spark: SparkSession, streamxConf: StreamxConf)
        extends AbstractPlugin(ssc, spark, streamxConf) {
    /**
      * 初始化文件系统对应的目录
      *
      * @param failDir
      */
    def initFailDir(failDir: String): Unit

    /**
      * 存储错误数据到指定文件系统的目录下
      *
      * @param df
      * @param failMeta
      */
    def saveFailFiles(df: DataFrame, failMeta: String): Unit
}
