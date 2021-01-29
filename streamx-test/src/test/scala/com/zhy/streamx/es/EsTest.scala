package com.zhy.streamx.es

import java.util

import org.apache.commons.collections.MapUtils
import org.apache.http.HttpHost
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.junit.{Before, Test}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-06-04
  *  \* Time: 16:41
  *  \* Description: 
  *  \*/
class EsTest {
    var client: RestHighLevelClient = _
    var bulkRequest: BulkRequest = _
    var index = "zhy_test_job1_5"
    var `type` = "base"
    var valList: ArrayBuffer[String] = ArrayBuffer.empty[String]
    var sparkSession: SparkSession = _

    @Before
    def setUp() = {
        client = getClient("33.69.6.95:9221,33.69.6.96:9221,33.69.6.97:9221,33.69.6.98:9221", null, null, new util.HashMap[String, AnyRef])
        bulkRequest = new BulkRequest()
        for (a <- 1 to 100) {
            valList += "{\"latitude\":\"29.36710410554054\",\"id\":\"571827\",\"time\":\"2020-01-14 21:07:58\",\"userId\":\"17baa7d6-36ce-11ea-8582-fa163e800762\",\"longitude\":\"120.09751950568165\",\"sendTime\":\"2020-03-19 12:03:45\"}"
        }

        System.setProperty("HADOOP_USER_NAME", "admin")
        sparkSession = SparkSession.builder()
                .master("local[8]")
                .appName("testtest123456789")
                .config("hive.metastore.uris", "thrift://33.69.6.23:9083")
                .enableHiveSupport()
                .getOrCreate()
    }

    @Test
    def testEs(): Unit = {
        for (a <- 1 to 3) {
            bulkRequest = new BulkRequest
            for (row <- valList) {
                var request = new IndexRequest(index, `type`)
                request = request.source(row, XContentType.JSON)
                bulkRequest.add(request)
            }
            val response = client.bulk(bulkRequest)
            if (response.hasFailures) {
                println(response.buildFailureMessage())
            }
        }
    }


    def getClient(address: String, username: String, password: String, config: util.Map[String, AnyRef]): RestHighLevelClient = {
        val httpHostList: util.List[HttpHost] = new util.ArrayList[HttpHost]
        val addr: Array[String] = address.split(",")
        for (add <- addr) {
            val pair: Array[String] = add.split(":")
            httpHostList.add(new HttpHost(pair(0), pair(1).toInt, "http"))
        }
        val builder: RestClientBuilder = RestClient.builder(httpHostList: _*)
        val timeout: Integer = MapUtils.getInteger(config, "timeout")
        if (timeout != null) builder.setMaxRetryTimeoutMillis(timeout * 1000)
        val pathPrefix: String = MapUtils.getString(config, "pathPrefix")
        //        if (StringUtils.isNotEmpty(pathPrefix)) builder.setPathPrefix(pathPrefix)
        new RestHighLevelClient(builder)
    }
}
