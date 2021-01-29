package com.zhy.streamx.exchange

import java.nio.file.Paths

import cn.tongdun.streamx.core.StreamCollectorConfigFile
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Before, Test}
/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2021-01-26
  *  \* Time: 19:14
  *  \* Description: 
  *  \*/
class StdoutTest {
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
        val path = Paths.get("src", "test/resources/data/stdout/datacollect_exit_entry_out.json").toAbsolutePath.toString
        val strings = Array("local:"+path)
        new StreamCollectorConfigFile().runJob(sparkSession, strings)
    }
}
