package cn.tongdun.streamx.other

import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.{AnalysisException, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Before, Test}

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-05-25
  *  \* Time: 18:18
  *  \* Description: 
  *  \*/
class FailoverTest {
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
        var textIn = sparkSession.sparkContext.textFile("/user/backup/app1/fail_1590401660206.fail")
        textIn.foreach(str => {
            val cr = str.asInstanceOf[ConsumerRecord[String, String]]
            println(cr)
        })
        //        textIn.foreachPartition(rdd => {
        //            if (rdd.nonEmpty) {
        //                println(rdd)
        //            }
        //        })
    }

    @Test
    def test2(): Unit = {
        var textIn = sparkSession.read.parquet("/user/backup/app1/fail_1590477300490.fail")
        println(textIn)
        //schema 用新的替代
        textIn.foreach(str => {
            println(str)
//            val cr = str.asInstanceOf[ConsumerRecord[String, String]]
//            println(cr)
        })
        //        textIn.foreachPartition(rdd => {
        //            if (rdd.nonEmpty) {
        //                println(rdd)
        //            }
        //        })
    }

    @Test
    def test3(): Unit = {
        val hdfs = org.apache.hadoop.fs.FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
        val failDirPath = new Path("/user/backup/app1")
        val files = hdfs.listStatus(failDirPath)
        for(fileStatus <- files) {
            val cPath = fileStatus.getPath
            println(cPath)
        }
        hdfs.close()
    }

    @Test
    def testErr(): Unit = {
        val errPath = "/user/backup/app1/hhy_dw.test_zhy1_tab_dt/fail_1591006106580.fail"
//        val errPath = "/user/backup/app1/hhy_dw.test_zhy1_tab_dt/fail_1591008086341.fail"
        try {
            val errDf = sparkSession.read.parquet(errPath)
            println(errDf)
        } catch {
            case e: AnalysisException => {
                e.printStackTrace()
            }
        }
    }
}
