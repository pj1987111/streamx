package com.zhy.streamx.core.entity

import com.zhy.streamx.core.api.parser.Parser
import com.zhy.streamx.core.api.writer.AbstractWriter
import com.zhy.streamx.core.constants.Key
import com.zhy.streamx.core.util.{ConfUtil, StreamUtils}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-06-02
  *  \* Time: 11:02
  *  \* Description: 用于广播变量存储全局配置信息
  *  \*/
class ExchangeDataConf(spark: SparkSession, configConf: ConfigConf) extends Serializable {
    /**
      * 源端数据解析器
      */
    var parser: Parser = _
    /**
      * 目标端
      */
    var writerBuffer: ArrayBuffer[AbstractWriter] = ArrayBuffer.empty[AbstractWriter]

    def init(): ExchangeDataConf = {
        //为了初始化tblTable
        StreamUtils.initReader(configConf.getReader.get(Key.NAME).asInstanceOf[String],
            null, spark, configConf, ConfUtil.toStr(configConf.getReader))
        parser = StreamUtils.initParser(configConf.getReader.get(Key.READER_PARSER).asInstanceOf[String])
        for (writer <- configConf.getWriters) {
            writerBuffer += StreamUtils.initWriter(
                writer.get(Key.NAME).asInstanceOf[String],
                spark, configConf, ConfUtil.toStr(writer))
        }
        this
    }

    def getTransformerConf: TransformerConf = configConf.getTransformer

    def getParser: Parser = parser

    def getWriterBuffer: ArrayBuffer[AbstractWriter] = writerBuffer
}
