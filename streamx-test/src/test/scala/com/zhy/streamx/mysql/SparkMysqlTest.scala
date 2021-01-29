package com.zhy.streamx.mysql

import java.util

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.codehaus.jackson.`type`.TypeReference
import org.codehaus.jackson.map.{DeserializationConfig, ObjectMapper}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.junit.{Before, Test}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-07-02
  *  \* Time: 10:49
  *  \* Description: 
  * upsert
  * insert into uu(id, name, password) values(5, '5_3', '5_3') on duplicate key update id=values(id),name=values(name),password=values(password)
  *  \*/
class SparkMysqlTest extends Serializable {
    @transient private lazy val mapper = new ObjectMapper()
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    var spark: SparkSession = _

    @Before
    def setUp() = {
        System.setProperty("HADOOP_USER_NAME", "admin")
        spark = SparkSession.builder()
                .master("local[8]")
                .appName("testtest123456")
                .config("hive.metastore.uris", "thrift://33.69.6.23:9083")
                .enableHiveSupport()
                .getOrCreate()
    }

    def createDf(): DataFrame = {
        def processOtherSchame(): StructType = {
            val structArray = new util.ArrayList[StructField]
            structArray.add(StructField("id", IntegerType, false))
            structArray.add(StructField("transId", StringType, true))
            structArray.add(StructField("table_type", StringType, true))
            structArray.add(StructField("model_code", StringType, true))
            structArray.add(StructField("model_version", StringType, true))
            structArray.add(StructField("datestr", StringType, true))
            StructType(structArray)
        }

        val rowRDD = new util.ArrayList[Row]()
        val structType = processOtherSchame()
        for (i <- 1 to 160) {
            rowRDD.add(Row(i, s"00$i", s"$i", s"code$i", s"$i", "20200529"))
        }
        println(s"rowRDD=$rowRDD, structType=$structType ")
        val scDataFram = spark.createDataFrame(rowRDD, structType)
        scDataFram
    }

    @Test
    def create(): Unit = {
        val tmpView = "testtestview"
        val scDataFram = createDf()
        //val df = scDataFram.toDF("table_type", "model_code", "model_version", "datestr")
        scDataFram.createOrReplaceTempView(tmpView)
        val values = spark.sql(s"select * from $tmpView").rdd.collect()
        for (value <- values)
            println(value)
        scDataFram
    }

    @Test
    def mysqlTest1(): Unit = {
        val df = createDf()
        val prop = new java.util.Properties
        prop.setProperty("user", "root")
        prop.setProperty("password", "Td@123456")

        df.write.mode(SaveMode.Append).jdbc("jdbc:mysql://33.69.6.167:3306/road", "testtab", prop)
    }

    @Test
    def partitionWrite(): Unit = {
        val topic = "zhytest1"
        val kafkaParams = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "10.57.34.20:9092",
            ConsumerConfig.GROUP_ID_CONFIG -> "zhytest2",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
            ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG -> "30000",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (true: java.lang.Boolean)
        )
        val ssc = new StreamingContext(spark.sparkContext, Seconds(20))
        val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](Set(topic), kafkaParams)
        )
        fun2(kafkaStream)
        ssc.start
        ssc.awaitTermination
    }

    def fun1(kafkaStream: InputDStream[ConsumerRecord[String, String]]): Unit = {
        //schema+table
        def getKey(valMap: Map[String, Object]): String = {
            "" + valMap.get("table")
        }

        kafkaStream.foreachRDD(rdd => {
            rdd.map(r => {
                implicit val formats: DefaultFormats.type = DefaultFormats
                val valMap = parse(r.value()).extract[Map[String, Object]]
                val key: String = getKey(valMap)
                (key, valMap)
            }).groupByKey().foreachPartition(iter => {
                if (iter.nonEmpty) {
                    iter.foreach(record => {
                        println("test...")
                        //key-map
                        val sortMap = mutable.HashMap[String, Map[String, Object]]()
                        for (value: Map[String, Object] <- record._2) {
                            val id = "" + value("id")
                            val sortKey = ("" + value("time")).toInt
                            if (sortMap.contains(id)) {
                                val keyVal = sortMap(id)
                                if (sortKey > ("" + keyVal("time")).toInt) {
                                    sortMap.put(id, value)
                                }
                            } else {
                                sortMap.put(id, value)
                            }
                        }
                        println(sortMap)
                    })
                }
            })
        })
    }

    def fun2(kafkaStream: InputDStream[ConsumerRecord[String, String]]): Unit = {
        //schema+table  加上id的话batch太少
        def getKey(valMap: util.Map[String, Object]): String = {
            "" + valMap.get("table")
        }

        def cols(): ArrayBuffer[String] = {
            val cols = ArrayBuffer.empty[String]
            cols += "latitude"
            cols += "longitude"
            cols += "id"
            cols += "time"
            cols += "userId"
            cols += "sendTime"
            cols += "table"
            cols
        }

        def schemaStructType(): StructType = {
            val structArray = new util.ArrayList[StructField]
            structArray.add(StructField("latitude", StringType, false))
            structArray.add(StructField("longitude", StringType, true))
            structArray.add(StructField("id", StringType, true))
            structArray.add(StructField("time", StringType, true))
            structArray.add(StructField("userId", StringType, true))
            structArray.add(StructField("sendTime", StringType, true))
            structArray.add(StructField("table", StringType, true))
            StructType(structArray)
        }

        kafkaStream.foreachRDD(rdd => {
            val df = spark.createDataFrame(
                rdd.flatMap(r => {
                    val rowsAb = ArrayBuffer.empty[(String, util.Map[String, Object])]
                    val msgVal = r.value()
                    if (msgVal.charAt(0) == '[' && msgVal.charAt(msgVal.length - 1) == ']') {
                        for(line<-JSON.parseArray(msgVal)) {
                            val valMap: util.Map[String, Object] = mapper.readValue(line.toString, new TypeReference[util.Map[String, Object]]() {})
                            val key: String = getKey(valMap)
                            rowsAb += ((key, valMap))
                        }
                    } else {
                        val valMap: util.Map[String, Object] = mapper.readValue(r.value(), new TypeReference[util.Map[String, Object]]() {})
                        val key: String = getKey(valMap)
                        rowsAb += ((key, valMap))
                    }
                    rowsAb
                }).groupByKey()  //必须groupbykey 将根据key将数据分布
                    //对接接口
                    .mapPartitions(rows => {
                    val rowsAb = ArrayBuffer.empty[Row]
                    if (rows.nonEmpty) {
                        val sortMap = mutable.HashMap[String, util.Map[String, Object]]()
                        for (row <- rows) {
                            //key-map
                            for (value: util.Map[String, Object] <- row._2) {
                                val id = "" + value.get("id")
                                val sortKey = ("" + value.get("time")).toInt
                                if (sortMap.contains(id)) {
                                    val keyVal = sortMap(id)
                                    if (sortKey > ("" + keyVal.get("time")).toInt) {
                                        sortMap.put(id, value)
                                    }
                                } else {
                                    sortMap.put(id, value)
                                }
                            }
                        }
                        for (colVal <- sortMap.values) {
                            val colValAb = ArrayBuffer.empty[Any]
                            for (col <- cols()) {
                                colValAb += colVal.get(col)
                            }
                            rowsAb += Row.fromSeq(colValAb)
                        }
                    }
                    rowsAb.toIterator
                }), schemaStructType())
            df.show()
        })
    }

    @Test
    def testMapArray(): Unit = {
        def getKey(valMap: util.Map[String, Object]): String = {
            "" + valMap.get("table")
        }
        var arrayRDD = spark.sparkContext.parallelize(Array("[{\"latitude\":\"29.36710410554054\",\"id\":\"1\",\"time\":\"10\",\"userId\":\"1-10\",\"longitude\":\"120.09751950568165\",\"sendTime\":\"2020-03-19 12:03:45\",\"table\":\"1\"},{\"latitude\":\"29.207429401408184\",\"id\":\"1\",\"time\":\"7\",\"userId\":\"1-7\",\"longitude\":\"119.83009850567478\",\"sendTime\":\"2020-03-19 12:03:47\",\"table\":\"1\"},{\"latitude\":\"28.86657478787006\",\"id\":\"2\",\"time\":\"2\",\"userId\":\"2-2\",\"longitude\":\"119.91622241952693\",\"sendTime\":\"2020-03-19 12:03:47\",\"table\":\"1\"},{\"latitude\":\"29.36710410554054\",\"id\":\"3\",\"time\":\"3\",\"userId\":\"3-3\",\"longitude\":\"120.09751950568165\",\"sendTime\":\"2020-03-19 12:03:45\",\"table\":\"1\"}]",
            "[{\"latitude\":\"29.207429401408184\",\"id\":\"3\",\"time\":\"4\",\"userId\":\"3-4\",\"longitude\":\"119.83009850567478\",\"sendTime\":\"2020-03-19 12:03:47\",\"table\":\"1\"},{\"latitude\":\"28.86657478787006\",\"id\":\"1\",\"time\":\"20\",\"userId\":\"1-20\",\"longitude\":\"119.91622241952693\",\"sendTime\":\"2020-03-19 12:03:47\",\"table\":\"1\"},{\"latitude\":\"29.36710410554054\",\"id\":\"3\",\"time\":\"3\",\"userId\":\"3-3\",\"longitude\":\"120.09751950568165\",\"sendTime\":\"2020-03-19 12:03:45\",\"table\":\"1\"},{\"latitude\":\"29.207429401408184\",\"id\":\"4\",\"time\":\"1\",\"userId\":\"4-1\",\"longitude\":\"119.83009850567478\",\"sendTime\":\"2020-03-19 12:03:47\",\"table\":\"2\"},{\"latitude\":\"28.86657478787006\",\"id\":\"4\",\"time\":\"2\",\"userId\":\"4-2\",\"longitude\":\"119.91622241952693\",\"sendTime\":\"2020-03-19 12:03:47\",\"table\":\"2\"}]"))
        arrayRDD.foreach(println)
        arrayRDD.flatMap(string=>{
            val rowsAb = ArrayBuffer.empty[(String, util.Map[String, Object])]
            for(line<-JSON.parseArray(string)) {
                val valMap: util.Map[String, Object] = mapper.readValue(line.toString, new TypeReference[util.Map[String, Object]]() {})
                val key: String = getKey(valMap)
                rowsAb += ((key, valMap))
            }
            rowsAb
        }).foreach(println)
    }
}
