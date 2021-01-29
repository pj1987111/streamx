package com.zhy.streamx.es

import org.apache.spark.sql.SparkSession
import org.junit.{Before, Test}

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-07-03
  *  \* Time: 14:22
  *  \* Description: 
  *  \*/
class EsRead {
    var sparkSession: SparkSession = _

    @Before
    def setUp() = {
        System.setProperty("HADOOP_USER_NAME", "admin")
        sparkSession = SparkSession.builder()
                .master("local[8]")
                .appName("testtest123456789")
                .getOrCreate()
    }

//    @Test
//    def testEsRead(): Unit = {
//        val esCfg = Map("es.nodes" -> "33.69.6.98",
//            "es.port" -> "9221",
//            "es.nodes.wan.only" -> "false")
//        val rdd = sparkSession.sparkContext.esRDD("gantry/baseInfo", esCfg)
//        val vals = rdd.collect()
//        for(value<-vals) {
//            println(value)
//        }
//    }
}
