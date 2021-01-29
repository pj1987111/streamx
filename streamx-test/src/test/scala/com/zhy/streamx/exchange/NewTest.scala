package com.zhy.streamx.exchange

import cn.tongdun.streamx.core.StreamCollectorConfigFile
import org.apache.spark.sql.SparkSession
import org.junit.{Before, Test}

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-05-25
  *  \* Time: 11:42
  *  \* Description: 
  *  \*/
class NewTest {
    var sparkSession: SparkSession = _

    @Before
    def setUp() = {
        System.setProperty("HADOOP_USER_NAME", "admin")
        sparkSession = SparkSession.builder()
                .master("local[8]")
                .appName("testtest123456")
                .config("hive.metastore.uris", "thrift://33.69.6.23:9083")
                .enableHiveSupport()
                .getOrCreate()
    }

    @Test
    def test1(): Unit = {
        val strings = Array("local:/Volumes/workspace/zhy/pass_workspace/streamx/streamx-test/src/test/resources/data/${project.version}/hive_test1.json")
        new StreamCollectorConfigFile().runJob(sparkSession, strings)
    }

    @Test
    def test1_2(): Unit = {
        val strings = Array("local:/Volumes/workspace/zhy/pass_workspace/streamx/streamx-test/src/test/resources/data/${project.version}/hive_test1_2.json")
        new StreamCollectorConfigFile().runJob(sparkSession, strings)
    }

    @Test
    def test2(): Unit = {
        val strings = Array("local:/Volumes/workspace/zhy/pass_workspace/streamx/streamx-test/src/test/resources/data/${project.version}/hive_test_entry_exit.json")
        new StreamCollectorConfigFile().runJob(sparkSession, strings)
    }

    @Test
    def test3(): Unit = {
        val strings = Array("local:/Volumes/workspace/zhy/pass_workspace/streamx/streamx-test/src/test/resources/data/${project.version}/hive_test_entry_exit_full.json")
        new StreamCollectorConfigFile().runJob(sparkSession, strings)
    }
}
