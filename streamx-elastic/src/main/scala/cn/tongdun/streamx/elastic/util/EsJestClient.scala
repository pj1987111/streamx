package cn.tongdun.streamx.elastic.util

import java.text.MessageFormat
import java.util.UUID
import java.{lang, util}

import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.codehaus.jackson.map.ObjectMapper
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.xcontent.XContentType

import scala.collection.mutable.ArrayBuffer

/**
  * @Author hongyi.zhou
  * @Description
  * @Date create in 2020-04-15. 
  **/
object EsJestClient {

    def trueIndexName(index:String, partitionValue: String): String = {
        var trueIndexName = index
        if (StringUtils.isNotBlank(partitionValue)) {
            trueIndexName = String.format(index, partitionValue.split(","): _*)
        }
        trueIndexName.toLowerCase
    }

    def changeIndexName(index: String, partitionValue: String, valueMap: util.HashMap[String, Object],
                        indexRequest: IndexRequest): Unit = {
        if (StringUtils.isNotBlank(partitionValue)) {
            val partAb = ArrayBuffer.empty[String]
            for (partV: String <- partitionValue.split(",")) {
                partAb.append("" + valueMap.getOrDefault(partV, ""))
            }
            val formatIndex = MessageFormat.format(index, partAb: _*).toLowerCase
            indexRequest.index(formatIndex)
        }
    }

    def bulkInsert(logger: Logger,
                   esClient: RestHighLevelClient,
                   index: String,
                   typo: String,
                   iterator: Iterator[Row],
                   schemaList: List[StructField],
                   batchSize: Int,
                   hasId: Boolean,
                   partitionValue: String): Unit = {
        //        var builder = new Bulk.Builder().defaultIndex(index.toLowerCase).defaultType(typo)
        var bulkRequest = new BulkRequest()
        var count = 0
        iterator.foreach(row => {
            val valueMap = getVal(row, schemaList)
            val value = new ObjectMapper().writeValueAsString(valueMap)
            var indexRequest = new IndexRequest(index, typo)
            indexRequest = if (hasId) new IndexRequest(index, typo, row.get(0).toString) else new IndexRequest(index, typo, getUUID32)
            changeIndexName(index, partitionValue, valueMap, indexRequest)
            indexRequest = indexRequest.source(value, XContentType.JSON)
            bulkRequest.add(indexRequest)
            count += 1
            if (count % batchSize == 0) {
                val response = esClient.bulk(bulkRequest)
                if (response.hasFailures) {
                    logger.error(response.buildFailureMessage())
                }
                bulkRequest = new BulkRequest
                count = 0
            }
        })
        if (count > 0) {
            val response = esClient.bulk(bulkRequest)
            if (response.hasFailures) {
                logger.error(response.buildFailureMessage())
            }
        }
    }

    private def getVal(row: Row, schemaList: List[StructField]) = {
        val map = new util.HashMap[String, Object]
        for (index <- 0 until row.length) {
            if (row.get(index) != null) {
                getObjVal(row, index, schemaList.apply(index), map)
            }
        }
        map
    }

    private def getObjVal(r: Row, index: Int, sField: StructField, map: util.HashMap[String, Object]) = {
        sField.dataType match {
            case IntegerType => map.put(sField.name, new Integer(r.getInt(index)))
            case LongType => map.put(sField.name, new java.lang.Long(r.getLong(index)))
            case DoubleType => map.put(sField.name, new lang.Double(r.getDouble(index)))
            case ShortType => map.put(sField.name, new java.lang.Short(r.getShort(index)))
            case FloatType => map.put(sField.name, new java.lang.Float(r.getFloat(index)))
            case BooleanType => map.put(sField.name, new java.lang.Boolean(r.getBoolean(index)))
            case _ => map.put(sField.name, r.get(index).toString)
        }
    }

    def getUUID32: String = {
        UUID.randomUUID.toString.replace("-", "").toLowerCase
    }
}
