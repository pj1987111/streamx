package com.zhy.streamx.tools

import java.sql.{Connection, SQLException}
import java.util

import com.zhy.streamx.mysql.writer.util.DbUtil
import org.apache.spark.sql.SparkSession
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
class SchemaJdbcReader {
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
                .getOrCreate()
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    }

    @throws[SQLException]
    def getFullColumns(table: String, dbConn: Connection): ArrayBuffer[(String, String)] = {
        val ret = ArrayBuffer.empty[(String, String)]
        val rs = dbConn.getMetaData.getColumns(null, null, table, null)
        while (rs.next) {
            ret += ((rs.getString("COLUMN_NAME"), rs.getString("TYPE_NAME")))
        }
        ret
    }

    @Test
    def test1() = {
        val url = "jdbc:mysql://33.69.6.167:3306/station_info?useSSL=false"
        val password = "Td@123456"
        //        val url = "jdbc:mysql://10.57.22.115:3306/test?useSSL=false"
        //        val password = "TD@123"
        val user = "root"
        val conn = DbUtil.getConnection(url, user, password)
        val tables = Array(
            //            "dfs_gantry_monitor_status"
            "all_road_fare",
            "entry_jour",
            "exit_jour",
            "lane_status_info",
            "weight_jour"
        )
        for (table <- tables) {
            val mapM = new util.LinkedHashMap[String, Object]()
            val fieldM = ArrayBuffer.empty[String]
            val cols = getFullColumns(table, conn)
            for ((name, colType) <- cols) {
                val typo = colType match {
                    case "TINYINT" => "int"
                    case "SMALLINT" => "int"
                    case "INT" => "int"
                    case "BIGINT" => "bigint"
                    case "FLOAR" => "double"
                    case "DOUBLE" => "double"
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
        DbUtil.closeDbResources(conn = conn)
    }
}
