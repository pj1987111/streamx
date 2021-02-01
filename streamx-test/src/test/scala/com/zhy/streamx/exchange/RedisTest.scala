package com.zhy.streamx.exchange

import com.zhy.streamx.core.StreamCollectorConfigFile
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Before, Test}

/**
  * @Author hongyi.zhou
  * @Description
  * @Date create in 2020-04-15. 
  **/
class RedisTest {
    var sparkSession: SparkSession = _

    @Before
    def setUp() = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("test")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        sparkSession = sqlContext.sparkSession
    }

    @Test
    def test1(): Unit = {
        val strings = Array("local:data/redis/redis_test1.json")
        new StreamCollectorConfigFile().runJob(sparkSession, strings)
    }

    @Test
    def test2(): Unit = {
        val strings = Array("local:/Volumes/workspace/zhy/pass_workspace/streamx/streamx-test/src/test/resources/data/redis/redis_test2.json")
        new StreamCollectorConfigFile().runJob(sparkSession, strings)
    }
}
