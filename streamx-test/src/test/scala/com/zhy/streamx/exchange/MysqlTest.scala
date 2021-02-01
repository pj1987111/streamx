package com.zhy.streamx.exchange

import com.zhy.streamx.core.StreamCollectorConfigFile
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Before, Test}

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-06-03
  *  \* Time: 19:12
  *  \* Description: 
  *  \*/
class MysqlTest {
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
        val strings = Array("local:/Volumes/workspace/zhy/pass_workspace/streamx/streamx-test/src/test/resources/data/${project.version}/mysql/2mysql.json")
        new StreamCollectorConfigFile().runJob(sparkSession, strings)
    }

    @Test
    def hhyTest1(): Unit = {
        val strings = Array("local:/Volumes/workspace/zhy/pass_workspace/streamx/streamx-test/src/test/resources/data/${project.version}/mysql/datacollect_merge_mysql_td.json")
        new StreamCollectorConfigFile().runJob(sparkSession, strings)
    }

    @Test
    def hhyTest2(): Unit = {
        val strings = Array("local:/Volumes/workspace/zhy/pass_workspace/streamx/streamx-test/src/test/resources/data/${project.version}/mysql/mysql_stream_station_info.json")
        new StreamCollectorConfigFile().runJob(sparkSession, strings)
    }
}
