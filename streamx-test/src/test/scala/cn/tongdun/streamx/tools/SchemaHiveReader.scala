package cn.tongdun.streamx.tools

import java.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.codehaus.jackson.map.{DeserializationConfig, ObjectMapper}
import org.junit.{Before, Test}

import scala.collection.mutable.ArrayBuffer

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-05-09
  *  \* Time: 16:57
  *  \* Description: 
  *  \*/
class SchemaHiveReader {
    var sparkSession: SparkSession = _
    @transient private lazy val mapper = new ObjectMapper()

    @Before
    def setUp(): Unit = {
        //设置读取的用户，不设置的话默认是本机登录用户
        System.setProperty("HADOOP_USER_NAME", "admin")
        //读取hive数据只需要设定hive.metastore.uris，然后把core-site.xml,hdfs-site.xml和hive-site.xml放到环境变量下即可
        sparkSession = SparkSession.builder()
                .master("local[8]")
                .appName("testtest123456")
                .config("hive.metastore.uris", "thrift://33.69.6.23:9083")
                .enableHiveSupport()
                .getOrCreate()
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    }

    @Test
    def test1() = {
        val tables = Array(
//            "hhy_dw.dsp_province_police_bayonet_dt"
            "hhy_dw.ods_province_police_traffic_avi_dt"
            //            "hhy_dw.ods_cps_rt_exit_balance_mt"
        )
        for (table <- tables) {
            val schemas = sparkSession.table(table).schema
            val mapM = new util.LinkedHashMap[String, Object]()
            val fieldM = ArrayBuffer.empty[String]
            for (schema <- schemas) {
                //                val name = "data." + schema.name
                val name = schema.name
                val typo = schema.dataType match {
                    case IntegerType => "int"
                    case ShortType => "int"
                    case LongType => "bigint"
                    case FloatType => "double"
                    case DoubleType => "double"
                    case _ => "string"
                }
                val innerMap = new util.LinkedHashMap[String, Object]()
                innerMap.put("type", typo)
                mapM.put(name, innerMap)
                fieldM.append("\"" + name.replaceAll("\\.", "_") + "\"")
            }

            println("table name : " + table)
            val jsonString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mapM)
            println(jsonString)
            println()
            println(fieldM.toList)
        }
    }

    @Test
    def test2(): Unit = {
        val str = "1599530245472"
        println(str.toLong)
    }
}
