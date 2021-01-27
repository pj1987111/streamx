package cn.tongdun.streamx.core.config

import cn.tongdun.streamx.core.constants.Key
import cn.tongdun.streamx.core.entity.StreamxConf
import cn.tongdun.streamx.core.util.{ConfUtil, StreamUtils}
import com.google.common.base.Joiner
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-05-29
  *  \* Time: 14:22
  *  \* Description: 
  *  \*/
object ConfigParser {

    /**
      * 加载配置
      *
      * @param configParam
      * @return
      */
    def reload(configParam: String): StreamxConf = {
        val streamxConf = ConfUtil.getWholeConf(configParam, classOf[StreamxConf])
        require(streamxConf.params != null, s"field ${Key.PARAMS} must be exists!")
        require(streamxConf.params.batchDuration != null, s"field ${Key.PARAMS_BATCHDURATION} must be exists!")
        require(streamxConf.configs != null, s"field ${Key.CONFIGS} must be exists!")

        for (confConfig <- streamxConf.getConfigs) {
            require(confConfig.getWriters != null, s"field ${Key.WRITERS} must be exists!")

            val kafkaReaderConf = confConfig.getReader
            require(kafkaReaderConf != null, s"field ${Key.READER} must be exists!")

            val transformer = confConfig.getTransformer
            require(transformer != null, s"field ${Key.TRANSFORMER} must be exists!")
            require(transformer.getReader_fields != null, s"field ${Key.TRANSFORMER_READER_FIELDS} must be exists!")
            require(transformer.getWriter_fields != null, s"field ${Key.TRANSFORMER_WRITER_FIELDS} must be exists!")
            transformer.transformerFields = if (transformer.getWriter_fields != null) Joiner.on(",").join(transformer.getWriter_fields) else "*"

            val schemaAb = ArrayBuffer.empty[(String, DataType)]
            for (readerField <- transformer.getReader_fields.entrySet()) {
                val columnName = readerField.getKey
                val thisType = readerField.getValue.get(Key.COLUMN_TYPE)
                if (StringUtils.isNotBlank(thisType)) {
                    thisType.toLowerCase match {
                        case Key.COLUMN_TYPE_TINYINT => schemaAb += columnName -> IntegerType
                        case Key.COLUMN_TYPE_SMALLINT => schemaAb += columnName -> IntegerType
                        case Key.COLUMN_TYPE_INT => schemaAb += columnName -> IntegerType
                        case Key.COLUMN_TYPE_BIGINT => schemaAb += columnName -> LongType
                        case Key.COLUMN_TYPE_LONG => schemaAb += columnName -> LongType
                        case Key.COLUMN_TYPE_BOOLEAN => schemaAb += columnName -> BooleanType
                        case Key.COLUMN_TYPE_FLOAT => schemaAb += columnName -> FloatType
                        case Key.COLUMN_TYPE_DOUBLE => schemaAb += columnName -> DoubleType
                        case Key.COLUMN_TYPE_STRING => schemaAb += columnName -> StringType
                        case _ => schemaAb += columnName -> StringType
                    }
                }
            }
            transformer.readerSchema = schemaAb
            //配置中嵌套字段.变成_
            transformer.transformerSchema = schemaAb.map(x => StructField(x._1.replaceAll("\\.", "_"), x._2))
            val transformerSort = transformer.getSort
            if (transformerSort != null) {
                transformerSort.partitionKeyIndex = getFieldIndexsInSchema(transformerSort.getPartitionKey, transformer.readerSchema)
                transformerSort.primaryKeyIndex = getFieldIndexsInSchema(transformerSort.getPrimaryKey, transformer.readerSchema)
                transformerSort.sortKeyIndex = getFieldIndexsInSchema(transformerSort.getSortKey, transformer.readerSchema)
            }
        }
        streamxConf
    }

    def getFieldIndexsInSchema(fields: String, schema: ArrayBuffer[(String, DataType)]): Array[Int] = {
        def getFieldIndexInSchema(field: String): Int = {
            if (field.length > 0) {
                for (index <- schema.indices) {
                    if (schema.get(index)._1.equals(field)) {
                        return index
                    }
                }
            }
            -1
        }

        if (fields.length == 0) {
            return Array[Int] {
                -1
            }
        }
        val fieldParts = fields.split(",")
        val res = new Array[Int](fieldParts.length)
        for (index <- fieldParts.indices) {
            res(index) = getFieldIndexInSchema(fieldParts(index))
        }
        res
    }

    def main(args: Array[String]): Unit = {
        val config = StreamUtils.readConfig("local:/Volumes/workspace/zhy/pass_workspace/streamx/streamx-test/src/test/resources/data/1.4.0/error_test.json")
        val streamxConf = reload(config)
        println(streamxConf)
    }
}
