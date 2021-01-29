package com.zhy.streamx.elastic.writer

import java.util

import com.zhy.streamx.core.api.writer.AbstractWriter
import com.zhy.streamx.core.entity.ConfigConf
import com.zhy.streamx.core.util.ConfUtil
import com.zhy.streamx.elastic.util.EsJestClient
import org.apache.commons.collections.MapUtils
import org.apache.http.HttpHost
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.client.{RestClient, RestClientBuilder, RestHighLevelClient}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * @Author hongyi.zhou
  *         https://www.elastic.co/guide/en/elasticsearch/hadoop/master/spark.html
  * @Description
  * @Date create in 2020-04-06. 
  **/
class ElasticWriter(spark: SparkSession, configConf: ConfigConf) extends AbstractWriter(spark, configConf) {
    @transient private lazy val logger = LogManager.getLogger(classOf[ElasticWriter])

    var elasticUrls: ArrayBuffer[String] = ArrayBuffer.empty[String]
    var index = ""
    var typo = ""
    var batchSize = 1000
    var hasId = false
    var partitionValue = ""
    var url = ""
    var port = ""
//    var esCfg: Map[String, String] = _

    override def init(cName: String, writerConfigStr: String): Unit = {
        this.cName = cName

        val writerConf = ConfUtil.getWholeConf(writerConfigStr, classOf[ElasticConf])
        for (url: String <- writerConf.getUrl.split(",")) {
            elasticUrls.append(url)
        }
        url = writerConf.getUrl
        port = writerConf.getPort
        typo = writerConf.getType
        batchSize = writerConf.getBatchSize
        hasId = writerConf.getHasId
        partitionValue = writerConf.getPartitionValue
//        index = EsJestClient.trueIndexName(writerConf.getIndex, partitionValue)
        index = writerConf.getIndex

//        esCfg = Map("pushdown" -> "true",
//            "es.nodes" -> url,
//            "es.port" -> port,
//            "es.nodes.wan.only" -> "false")
//        if (hasId) {
//            esCfg += ("es.mapping.id" -> configConf.transformer.readerSchema.apply(0)._1.replaceAll("\\.", "_"))
//        }
    }

    override def write(df: DataFrame): Unit = {
        createView(df)
        val dfT = spark.sql(selectTmpSql())
        //        dfT.saveToEs(s"$index/$typo", esCfg)
        val schemaList = dfT.schema.toList
        dfT.foreachPartition(rows => {
            var client: RestHighLevelClient = null
            try {
                client = getClient(new util.HashMap[String, AnyRef])
                EsJestClient.bulkInsert(logger, client, index, typo, rows, schemaList, batchSize, hasId, partitionValue)
            } catch {
                case e: Exception => {
                    logger.error(s"error ${e.getMessage}", e)
                }
            } finally {
                if (null != client) client.close()
            }
        })
    }

    def getClient(config: util.Map[String, AnyRef]): RestHighLevelClient = {
        val httpHostList: util.List[HttpHost] = new util.ArrayList[HttpHost]
        val addr: Array[String] = url.split(",")
        for (add <- addr) {
            val pair: Array[String] = add.split(":")
            httpHostList.add(new HttpHost(pair(0), pair(1).toInt, "http"))
        }
        val builder: RestClientBuilder = RestClient.builder(httpHostList: _*)
        val timeout: Integer = MapUtils.getInteger(config, "timeout")
        if (timeout != null) builder.setMaxRetryTimeoutMillis(timeout * 1000)
        val pathPrefix: String = MapUtils.getString(config, "pathPrefix")
        new RestHighLevelClient(builder)
    }
}
