package com.zhy.streamx.core

import com.zhy.streamx.core.api.plugin.AbstractFailoverPlugin
import com.zhy.streamx.core.api.reader.RowStream
import com.zhy.streamx.core.api.writer.AbstractWriter
import com.zhy.streamx.core.entity.{ConfigConf, ExchangeDataConf, SortConf}
import com.zhy.streamx.core.plugins.PluginManager
import com.zhy.streamx.core.util.StreamUtils
import com.zhy.streamx.core.util.broadcast.BroadcastWrapperInstance
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-05-23
  *  \* Time: 16:38
  *  \* Description: 
  *  \*/
class ExchangeRunner(configConf: ConfigConf,
                     ssc: StreamingContext, spark: SparkSession) extends Serializable {
    @transient private lazy val logger = LogManager.getLogger(classOf[ExchangeRunner])

    def init(): ExchangeRunner = {
        logger.info(s"configParam is : $configConf")
        logger.info("开始处理数据")
        this
    }

    /**
      * 源端读取插件回调函数
      * 数据处理核心逻辑
      */
    def exchangeData[T](rdd: RDD[T], spark: SparkSession, rowStream: RowStream[T], index: Int): Unit = {
        if (!rdd.isEmpty()) {
            try {
                val exchangeDataConf = BroadcastWrapperInstance.getInstance(rdd.sparkContext).value.apply(index)
                val df = if (!isSort(exchangeDataConf.getTransformerConf.getSort)) {
                    exchangeDataDataFrame(rdd, rowStream, exchangeDataConf)
                } else {
                    exchangeDataSortDataFrame(rdd, rowStream, exchangeDataConf)
                }
                for (wb <- exchangeDataConf.getWriterBuffer) {
                    try {
                        wb.write(df)
                    } catch {
                        case e: Exception => {
                            logger.error(s"error ${e.getMessage}", e)
                            if (wb.saveFails()) {
                                saveFailFiles(df, wb.failMeta())
                            }
                        }
                    }
                }
            } catch {
                case e: Exception => {
                    logger.error(s"error ${e.getMessage}", e)
                }
            }
        }

        /**
          * 是否需要排序
          *
          * @param sort
          * @return
          */
        def isSort(sort: SortConf): Boolean = {
            sort != null && (sort.partitionKey.length > 0 || sort.primaryKey.length > 0 || sort.sortKey.length > 0)
        }

        /**
          * 存储错误数据到指定文件系统的目录下
          * 注:配1个，防止异常恢复写入多份重复数据
          *
          * @param df
          * @param failMeta
          */
        def saveFailFiles(df: DataFrame, failMeta: String): Unit = {
            val failoverPlugins = PluginManager.getFailoverInstance()
            if (failoverPlugins != null && failoverPlugins.nonEmpty) {
                for (failoverPlugin <- failoverPlugins) {
                    failoverPlugin.asInstanceOf[AbstractFailoverPlugin].saveFailFiles(df, failMeta)
                }
            }
        }
    }

    /**
      * 默认处理
      *
      * @param rdd
      * @param rowStream
      * @param exchangeDataConf
      * @tparam T
      * @return
      */
    private def exchangeDataDataFrame[T](rdd: RDD[T], rowStream: RowStream[T], exchangeDataConf: ExchangeDataConf): DataFrame = {
        val parser = exchangeDataConf.getParser
        val transformerConf = exchangeDataConf.getTransformerConf
        val transformerWhere = transformerConf.getWhere

        val df = spark.createDataFrame(
            rdd.mapPartitions(rows => {
                val rowsAb = ArrayBuffer.empty[Row]
                for (row <- rows) {
                    parser.parseRowWithCondition(rowStream.rowValue(row), transformerConf.readerSchema, transformerWhere, rowsAb)
                }
                rowsAb.toIterator
            }),
            new StructType(transformerConf.transformerSchema.toArray)
        )
        df
    }

    /**
      * 提前分组排序，用于mysql/delta等支持DML数据库
      *
      * @param rdd
      * @param rowStream
      * @param exchangeDataConf
      * @tparam T
      * @return
      */
    private def exchangeDataSortDataFrame[T](rdd: RDD[T], rowStream: RowStream[T], exchangeDataConf: ExchangeDataConf): DataFrame = {
        val parser = exchangeDataConf.getParser
        val transformerConf = exchangeDataConf.getTransformerConf
        val sortConf = transformerConf.getSort
        val transformerWhere = transformerConf.getWhere
        val transformerReaderSchema = transformerConf.readerSchema

        val df = spark.createDataFrame(
            rdd.flatMap(row => {
                val rowsAbRes = ArrayBuffer.empty[(String, Row)]
                val rowsAb = ArrayBuffer.empty[Row]
                parser.parseRowWithCondition(rowStream.rowValue(row), transformerReaderSchema, transformerWhere, rowsAb)
                for (rowValue <- rowsAb) {
                    val partitionKey: String = StreamUtils.getUserDefinedVal(rowValue, sortConf.partitionKeyIndex, "")
                    rowsAbRes += ((partitionKey, rowValue))
                }
                rowsAbRes
            }).groupByKey().mapPartitions(keyRows => {
                val rowsAb = ArrayBuffer.empty[Row]
                if (keyRows.nonEmpty) {
                    val sortMap = mutable.LinkedHashMap[String, Row]()
                    for (keyRow <- keyRows) {
                        for (value: Row <- keyRow._2) {
                            val id = StreamUtils.getUserDefinedVal(value, sortConf.primaryKeyIndex, "")
                            if (!sortMap.contains(id) ||
                                    StreamUtils.getUserDefinedVal(value, sortConf.sortKeyIndex, "0").toLong
                                            > StreamUtils.getUserDefinedVal(sortMap(id), sortConf.sortKeyIndex, "0").toLong) {
                                sortMap.put(id, value)
                            }
                        }
                    }
                    sortMap.values.foreach(colVal => rowsAb += colVal)
                }
                rowsAb.toIterator
            }),
            new StructType(transformerConf.transformerSchema.toArray)
        )
        df
    }
}

object ExchangeRunner {
    def execute(spark: SparkSession, df: DataFrame, ab: AbstractWriter, index: Int): Boolean = {
        var hasErr = false
        val exchangeDataConf = BroadcastWrapperInstance.getInstance(spark.sparkContext).value.apply(index)
        val transformerConf = exchangeDataConf.getTransformerConf

        try {
            ab.write(
                spark.createDataFrame(df.rdd,
                    new StructType(transformerConf.transformerSchema.toArray)
                )
            )
        } catch {
            case _: Exception => {
                hasErr = true
            }
        }
        hasErr
    }
}
