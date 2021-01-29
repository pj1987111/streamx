package com.zhy.streamx.core.parser

import java.util

import com.zhy.streamx.core.util.StreamUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType
import org.codehaus.jackson.`type`.TypeReference

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-07-06
  *  \* Time: 19:22
  *  \* Description: canal写入kafka格式
  *  \*/
class CanalParser extends JsonParser {
    //canal数据存储根节点
    val DATA_PREFIX = "data"
    val MYSQLTYPE_PREFIX = "mysqlType"
    val SQLTYPE_PREFIX = "sqlType"
    val TABLE_PREFIX = "table"
    val TYPE_PREFIX = "type"

    /**
      * 解析行数据
      * 将data下的数组平铺
      *
      * @param row    读取的一行数据
      * @param schema 配置参数
      * @param rowsAb 解析后返回值存储
      */
    override def parseRow(row: String, schema: ArrayBuffer[(String, DataType)], rowsAb: ArrayBuffer[Row]): Unit = {
        parseRowWithCondition(row, schema, null, rowsAb)
    }

    /**
      * 解析行数据+预过滤
      *
      * @param row              读取的一行数据
      * @param schema           配置参数
      * @param transformerWhere 过滤条件
      * @param rowsAb           解析后返回值存储
      */
    override def parseRowWithCondition(row: String, schema: ArrayBuffer[(String, DataType)], transformerWhere: String, rowsAb: ArrayBuffer[Row]): Unit = {
        def isNotFilter(rootValue: util.Map[String, Object]): Boolean = {
            !rootValue.get(TYPE_PREFIX).toString.equals("DELETE") &&
                    rootValue.get(TABLE_PREFIX).toString.equals(getWhereTableName(transformerWhere))
        }

        val msgVal = row.trim
        if (StringUtils.isNotBlank(msgVal)) {
            try {
                val resValue: util.List[util.Map[String, Object]] = new util.ArrayList[util.Map[String, Object]]()
                val rootNode = mapper.readTree(msgVal)
                val rootValue: util.Map[String, Object] = mapper.readValue(rootNode, new TypeReference[util.Map[String, Object]]() {})
                if (StringUtils.isBlank(transformerWhere) || isNotFilter(rootValue)) {
                    val dataListNodes = rootValue.get(DATA_PREFIX)
                    if (dataListNodes != null && dataListNodes.isInstanceOf[util.ArrayList[Object]]) {
                        for (dataListNode <- dataListNodes.asInstanceOf[util.ArrayList[Object]]) {
                            dataListNode match {
                                case innerVal: util.Map[String, Object] =>
                                    resValue.add(innerVal)
                                case _ =>
                            }
                        }
                    }
                    for (rootVal <- rootValue) {
                        if (!rootVal._1.equals(DATA_PREFIX) && !rootVal._1.equals(MYSQLTYPE_PREFIX) && !rootVal._1.equals(SQLTYPE_PREFIX)) {
                            for (resVal <- resValue) {
                                resVal.put(rootVal._1, rootVal._2)
                            }
                        }
                    }
                    BaseParser.parseMapRow(resValue, schema, rowsAb)
                }
            } catch {
                case _: Exception =>
            }
        }
    }

    def getWhereTableName(transformerWhere: String): String = {
        val resTabName = ""
        val conditionParts = transformerWhere.split("and")
        for (conditionPart <- conditionParts) {
            if (conditionPart.trim.startsWith(TABLE_PREFIX) && conditionPart.trim.contains("=")) {
                val literVal = StreamUtils.trimFirstAndLastChar(conditionPart.trim.substring(conditionPart.trim.lastIndexOf("=") + 1), "'")
                return literVal
            }
        }
        resTabName
    }
}