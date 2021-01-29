package com.zhy.streamx.core.parser

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-08-12
  *  \* Time: 15:00
  *  \* Description: 
  *  \*/
object BaseParser {

    def parseMapRow(rows: util.List[util.Map[String, Object]], schema: ArrayBuffer[(String, DataType)], rowsAb: ArrayBuffer[Row]): Unit = {
        try {
            for (row <- rows) {
                rowsAb += Row.fromSeq(parseMapSingleVal(schema, row))
            }
        } catch {
            case _: Exception =>
        }
    }

    def parseMapSingleVal(schema: ArrayBuffer[(String, DataType)], row: util.Map[String, Object]): ArrayBuffer[Any] = {
        val colValAb = ArrayBuffer.empty[Any]
        //遍历所有配置列到数据中找
        for ((field, valType) <- schema) {
            getFieldMapValue(row, colValAb, field, valType)
        }
        colValAb
    }

    def getFieldMapValue(row: util.Map[String, Object], colValAb: ArrayBuffer[Any], field: String, valType: DataType): Unit = {
        try {
            valType match {
                case IntegerType => colValAb += new Integer(row.get(field).toString)
                case LongType => colValAb += new java.lang.Long(row.get(field).toString)
                case BooleanType => colValAb += new java.lang.Boolean(row.get(field).toString)
                case FloatType => colValAb += new java.lang.Float(row.get(field).toString)
                case DoubleType => colValAb += new java.lang.Double(row.get(field).toString)
                case _ => colValAb += row.get(field).toString
            }
        } catch {
            case _: Exception =>
                getFieldValueDefaultValInner(colValAb, valType)
        }
    }

    def getFieldValueDefaultValInner(colValAb: ArrayBuffer[Any], valType: DataType): Unit = {
        valType match {
            case IntegerType => colValAb += 0
            case LongType => colValAb += 0L
            case BooleanType => colValAb += false
            case FloatType => colValAb += 0f
            case DoubleType => colValAb += 0d
            case StringType => colValAb += ""
            case _ => colValAb += ""
        }
    }
}
