package cn.tongdun.streamx.core.parser

import cn.tongdun.streamx.core.api.parser.Parser
import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.{DeserializationConfig, ObjectMapper}
import org.codehaus.jackson.node.TextNode

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * @Author hongyi.zhou
  * @Description json解析器
  * @Date create in 2020-02-26. 
  **/
class JsonParser extends Parser {
    @transient lazy val mapper = new ObjectMapper()
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false)

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
                // 数组
                if (msgVal.charAt(0) == '[' && msgVal.charAt(msgVal.length - 1) == ']') {
                    for (line <- JSON.parseArray(msgVal)) {
                        rowsAb += Row.fromSeq(parseSingleVal(schema, line.toString))
                    }
                }
                // 非数组
                else {
                    rowsAb += Row.fromSeq(parseSingleVal(schema, msgVal))
                }
            } catch {
                case _: Exception =>
            }
        }
    }

    /**
      * 处理单条数据逻辑
      *
      * @param schema 配置参数
      * @param line   单条数据
      * @return
      */
    def parseSingleVal(schema: ArrayBuffer[(String, DataType)], line: String): ArrayBuffer[Any] = {
        val colValAb = ArrayBuffer.empty[Any]
        //遍历所有配置列到数据中找
        for ((field, valType) <- schema) {
            //嵌套字段用.分隔
            getFieldValue(mapper.readTree(line), colValAb, field, valType)
        }
        colValAb
    }

    /**
      * 递归处理单个字段逻辑
      *
      * @param jsonNode 单条数据
      * @param colValAb 返回结果(所有列)存储
      * @param field    字段名
      * @param valType  字段类型
      */
    def getFieldValue(jsonNode: JsonNode, colValAb: ArrayBuffer[Any], field: String, valType: DataType): Unit = {
        try {
            //递归处理，支持无限层json嵌套
            if (field.contains(".")) {
                val parent = field.substring(0, field.indexOf("."))
                val child = field.substring(field.indexOf(".") + 1)
                getFieldValue(jsonNode.get(parent), colValAb, child, valType)
            } else {
                getFieldValueInner(jsonNode, colValAb, field, valType)
            }
        } catch {
            case _: Exception =>
                BaseParser.getFieldValueDefaultValInner(colValAb, valType)
        }
    }

    def getFieldValueInner(jsonNode: JsonNode, colValAb: ArrayBuffer[Any], field: String, valType: DataType): Unit = {
        require(jsonNode != null && jsonNode.get(field) != null)
        valType match {
            case IntegerType => colValAb += jsonNode.get(field).asInt(0)
            case LongType => colValAb += jsonNode.get(field).asLong(0L)
            case BooleanType => colValAb += jsonNode.get(field).asBoolean(false)
            case FloatType => colValAb += jsonNode.get(field).asDouble(0f)
            case DoubleType => colValAb += jsonNode.get(field).asDouble(0d)
            case _ => {
                val filedVal = jsonNode.get(field)
                if(filedVal.isInstanceOf[TextNode])
                    colValAb += ("" + filedVal.asText())
                else
                    colValAb += ("" + filedVal.toString)
            }
        }
    }
}