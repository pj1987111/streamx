package com.zhy.streamx.core.parser

import com.zhy.streamx.core.api.parser.Parser
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-05-30
  *  \* Time: 21:04
  *  \* Description: csv解析器(包括text多分隔符)
  *  \*/
class CsvParser extends Parser {
    var splitter: String = ","

    //todo 放入配置
    def setSplitter(splitter: String): Unit = {
        this.splitter = splitter
    }

    /**
      * 解析行数据
      *
      * @param row    读取的一行数据
      * @param schema 配置参数
      * @param rowsAb 解析后返回值存储
      */
    override def parseRow(row: String, schema: ArrayBuffer[(String, DataType)], rowsAb: ArrayBuffer[Row]): Unit = {
        val msgVal = row.trim
        if (StringUtils.isNotBlank(msgVal)) {
            try {
                val msgParts = msgVal.split(splitter)
                val colValAb = ArrayBuffer.empty[Any]
                for (((field, valType), index) <- schema.view.zipWithIndex) {
                    if (index < msgParts.size) {
                        getFieldValue(msgParts.apply(index), colValAb, field, valType)
                    } else {
                        BaseParser.getFieldValueDefaultValInner(colValAb, valType)
                    }
                }
                rowsAb += Row.fromSeq(colValAb)
            } catch {
                case _: Exception =>
            }
        }
    }

    /**
      * 递归处理单个字段逻辑
      *
      * @param colVal   单字段数据
      * @param colValAb 返回结果(所有列)存储
      * @param field    字段名
      * @param valType  字段类型
      */
    def getFieldValue(colVal: String, colValAb: ArrayBuffer[Any], field: String, valType: DataType): Unit = {
        try {
            valType match {
                case IntegerType => colValAb += colVal.toInt
                case LongType => colValAb += colVal.toLong
                case BooleanType => colValAb += colVal.toBoolean
                case FloatType => colValAb += colVal.toFloat
                case DoubleType => colValAb += colVal.toDouble
                case _ => colValAb += colVal
            }
        } catch {
            case _: Exception =>
                BaseParser.getFieldValueDefaultValInner(colValAb, valType)
        }
    }
}
