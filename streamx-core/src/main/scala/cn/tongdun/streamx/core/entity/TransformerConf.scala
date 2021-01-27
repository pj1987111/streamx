package cn.tongdun.streamx.core.entity

import java.util

import org.apache.spark.sql.types.{DataType, StructField}

import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-05-29
  *  \* Time: 14:14
  *  \* Description: 转换配置
  *  \*/
case class TransformerConf() extends Serializable {
    //配置文件中使用列
    /**
      * 临时表转换条件
      */
    @BeanProperty
    var where: String = ""

    @BeanProperty
    var sort:SortConf = _

    /**
      * 源读取列
      */
    @BeanProperty
    var reader_fields: util.LinkedHashMap[String, util.LinkedHashMap[String, String]] = _
    /**
      * 临时表写入字段(可用函数)
      */
    @BeanProperty
    var writer_fields: util.ArrayList[String] = _

    //新增程序使用列
    /**
      * 临时表转换用的列
      */
    var transformerFields: String = "*"
    /**
      * kafka解析数据用的schema
      */
    var readerSchema: ArrayBuffer[(String, DataType)] = ArrayBuffer.empty[(String, DataType)]
    /**
      * 临时表用的结构
      */
    var transformerSchema: ArrayBuffer[StructField] = ArrayBuffer.empty[StructField]

    override def toString = s"TransformerConf($where, $reader_fields, $writer_fields, $transformerFields, $readerSchema, $transformerSchema)"
}
