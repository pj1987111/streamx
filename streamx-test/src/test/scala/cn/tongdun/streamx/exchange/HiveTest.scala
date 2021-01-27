package cn.tongdun.streamx.exchange

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
class HiveTest {
    var sparkSession: SparkSession = _

    @Before
    def setUp() = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("test")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        sparkSession = sqlContext.sparkSession
    }

    @Test
    def testWriteHIve = {

    }
}
