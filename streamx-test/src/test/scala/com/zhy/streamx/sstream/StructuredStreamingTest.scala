package com.zhy.streamx.sstream

import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.junit.Test

import scala.concurrent.duration._

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-05-12
  *  \* Time: 17:40
  *  \* Description: 
  *  \*/
class StructuredStreamingTest {

    def initSpark() = {
        System.setProperty("HADOOP_USER_NAME", "admin")
        val spark = SparkSession.builder()
                .master("local[2]")
//                .master("spark://cdh22:7077")
                .appName("testStructuredStreaming")
                .getOrCreate()
        //        spark.sparkContext.setLogLevel("WARN")
        spark.sparkContext.setLogLevel("ERROR")
        spark
    }

    @Test
    def kafkaTest1() = {
        val spark = initSpark()
        import spark.implicits._

        val df = spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "33.69.6.13:9092,33.69.6.14:9092,33.69.6.15:9092,33.69.6.16:9092,33.69.6.17:9092,33.69.6.18:9092,33.69.6.19:9092,33.69.6.20:9092,33.69.6.21:9092,33.69.6.22:9092")
                .option("subscribe", "sstream")
                //默认是从lastest读，这里设置从头开始读
                .option("startingOffsets", "earliest")
                .load()
        val kafkaDf: Dataset[(String, String)] = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .as[(String, String)]
        //判断是否为流处理
        println(kafkaDf.isStreaming)
        kafkaDf.printSchema()
        val words = kafkaDf.flatMap(_._2.split(" "))
        val wordCounts = words.groupBy("value").count()
        val query = wordCounts
                .writeStream
                .outputMode("complete")
                .format("console")
                .trigger(ProcessingTime(5.seconds))
                .start()
        query.awaitTermination()

        //        val words = lines.as[String].flatMap(_.split(" "))
        //        val wordCounts = words.groupBy("value").count()
        ////        df1.printSchema()
        //        val query = wordCounts.writeStream.outputMode("complete").format("console").start()
        //        query.awaitTermination()
    }

    @Test
    def simpleTest2(): Unit = {
        val spark = initSpark()

        import spark.implicits._
        // 第二步: 创建流。配置从 socket 读取流数据，地址和端口为 localhost: 9999
        val lines: DataFrame = spark.readStream.format("socket")
                .option("host", "127.0.0.1")
                .option("port", "9999")
                .load()

        val lineDf: Dataset[(String, String)] = lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .as[(String, String)]

        // 第三步: 进行单词统计。这里 lines 是 DataFrame ，使用 as[String］给它定义类型转换为 Dataset, 之后在 Dataset 里进行单词统计。
        var words: Dataset[String] = lineDf.as[String].flatMap(_.split(" "))
        val wordcount: DataFrame = words.groupBy("value").count()

        // 第四步: 创建查询句柄，定义打印结果方式并启动程序 这里使用 writeStream 方法, 输出模式为全部输出到控制台。
        val query: StreamingQuery = words.writeStream
                .outputMode(OutputMode.Complete)
                .trigger(Trigger.ProcessingTime("3 seconds"))
                .format("console")
                .start()
        // 调用 awaitTermination 方法来防止程序在处理数据时停止
        query.awaitTermination()
    }
}
