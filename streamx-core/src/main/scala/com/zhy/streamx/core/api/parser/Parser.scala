package com.zhy.streamx.core.api.parser

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType

import scala.collection.mutable.ArrayBuffer

/**
  * @Author hongyi.zhou
  * @Description 所有解析器必须实现接口
  * @Date create in 2020-02-26.
  **/
trait Parser extends Serializable {
    /**
      * 解析行数据
      *
      * @param row    读取的一行数据
      * @param schema 配置参数
      * @param rowsAb 解析后返回值存储
      */
    def parseRow(row: String, schema: ArrayBuffer[(String, DataType)], rowsAb: ArrayBuffer[Row]): Unit

    /**
      * 解析行数据+预过滤
      *
      * @param row              读取的一行数据
      * @param schema           配置参数
      * @param transformerWhere 过滤条件
      * @param rowsAb           解析后返回值存储
      */
    def parseRowWithCondition(row: String, schema: ArrayBuffer[(String, DataType)], transformerWhere: String, rowsAb: ArrayBuffer[Row]): Unit = {
        parseRow(row, schema, rowsAb)
    }
}
