package cn.tongdun.streamx.exchange

import cn.tongdun.streamx.core.StreamCollectorConfigFile
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Before, Test}


/**
  * @Author hongyi.zhou
  * @Description
  * @Date create in 2020-04-15. 
  **/
class ElasticTest {
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
        val strings = Array("local:/Volumes/workspace/zhy/pass_workspace/streamx/streamx-test/src/test/resources/data/elastic/elastic_nodata.json")
        new StreamCollectorConfigFile().runJob(sparkSession, strings)
    }

    @Test
    def test2(): Unit = {
        val strings = Array("local:/Volumes/workspace/zhy/pass_workspace/streamx/streamx-test/src/test/resources/data/elastic/datacollect_exit_entry_es.json")
        new StreamCollectorConfigFile().runJob(sparkSession, strings)
    }
}
