package cn.tongdun.streamx.core.util

import java.io.{BufferedReader, FileInputStream, InputStream, InputStreamReader}
import java.net.URI
import java.util.Base64

import cn.tongdun.streamx.core.api.parser.Parser
import cn.tongdun.streamx.core.api.plugin.AbstractPlugin
import cn.tongdun.streamx.core.api.reader.AbstractReader
import cn.tongdun.streamx.core.api.writer.AbstractWriter
import cn.tongdun.streamx.core.config.ConfigParser
import cn.tongdun.streamx.core.constants.Key
import cn.tongdun.streamx.core.entity.{ConfigConf, StreamxConf}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
  * @Author hongyi.zhou
  * @Description
  * @Date create in 2020-01-11. 
  **/
object StreamUtils {
    val DEFAULT_READER_NAME = "cn.tongdun.streamx.kafka.reader.KafkaReader"

    @throws[Exception]
    def readConfig(fName: String): String = {
        fName match {
            case s if s.startsWith("local:") => readFromLocal(s.stripPrefix("local:"))
            case s if s.startsWith("hdfs:") => readFromHdfs(s.stripPrefix("hdfs:"))
            case _ => readFromHdfs(fName)
        }
    }

    @throws[Exception]
    private def readFromHdfs(fpname: String): String = {
        val path = new Path(fpname)
        val fs = FileSystem.get(new URI("hdfs://tdhdfs"), new Configuration)
        readFromStream(fs.open(path))
    }

    @throws[Exception]
    private def readFromLocal(fpname: String): String = {
        readFromStream(new FileInputStream(fpname))
    }

    @throws[Exception]
    private def readFromStream(is: InputStream): String = {
        val br = new BufferedReader(new InputStreamReader(is))
        val sb = new StringBuilder
        var line = br.readLine
        while ( {
            line != null
        }) {
            sb.append(line)
            line = br.readLine
        }
        br.close()
        sb.toString
    }

    @throws[Exception]
    def parseStreamxConf(dataStr: String): StreamxConf = {
        var confStr = dataStr
        if (!confStr.contains('{') && !confStr.contains('}')) {
            confStr = new String(Base64.getDecoder.decode(confStr), "utf-8")
        }
        ConfigParser.reload(confStr)
    }

    /**
      * 初始化Reader
      *
      * @param cName
      * @param ssc
      * @param spark
      * @param configConf
      * @param readerConfigStr
      * @return
      */
    def initReader(cName: String, ssc: StreamingContext, spark: SparkSession,
                   configConf: ConfigConf, readerConfigStr: String): AbstractReader = {
        var readerCname = cName
        if (StringUtils.isBlank(cName))
            readerCname = DEFAULT_READER_NAME

        val reader = Class.forName(readerCname)
                .getConstructor(classOf[StreamingContext], classOf[SparkSession], classOf[ConfigConf])
                .newInstance(ssc, spark, configConf)
                .asInstanceOf[AbstractReader]
        reader.init(cName, readerConfigStr)
        configConf.tblTable = reader.tblTable
        reader
    }

    /**
      * 初始化解析器
      *
      * @param cName
      * @return
      */
    def initParser(cName: String): Parser = {
        def instanceParserObj(clazzName: String): Parser = {
            Class.forName(clazzName)
                    .getConstructor()
                    .newInstance()
                    .asInstanceOf[Parser]
        }

        cName match {
            case Key.JSON_PARSER =>
                instanceParserObj("cn.tongdun.streamx.core.parser.JsonParser")
            case Key.CSV_PARSER =>
                instanceParserObj("cn.tongdun.streamx.core.parser.CsvParser")
            case Key.CANAL_PARSER =>
                instanceParserObj("cn.tongdun.streamx.core.parser.CanalParser")
            case _ =>
                instanceParserObj(cName)
        }
    }

    /**
      * 初始化writer
      *
      * @param cName
      * @param spark
      * @param configConf
      * @param writerConfigStr
      * @return
      */
    def initWriter(cName: String, spark: SparkSession,
                   configConf: ConfigConf, writerConfigStr: String): AbstractWriter = {
        val writer = Class.forName(cName)
                .getConstructor(classOf[SparkSession], classOf[ConfigConf])
                .newInstance(spark, configConf)
                .asInstanceOf[AbstractWriter]
        writer.init(cName, writerConfigStr)
        writer
    }

    /**
      * 初始化插件
      *
      * @param cName
      * @param ssc
      * @param spark
      * @param pluginConfigStr
      * @param streamxConf
      * @return
      */
    def initPlugin(cName: String, ssc: StreamingContext, spark: SparkSession,
                   pluginConfigStr: String, streamxConf: StreamxConf): AbstractPlugin = {
        val plugin = Class.forName(cName)
                .getConstructor(classOf[StreamingContext], classOf[SparkSession], classOf[StreamxConf])
                .newInstance(ssc, spark, streamxConf)
                .asInstanceOf[AbstractPlugin]
        plugin.init(cName, pluginConfigStr)
        plugin
    }

    /**
      * 根据index获取值
      *
      * @param row
      * @param indexs
      * @param defaultVal
      * @return
      */
    def getUserDefinedVal(row: Row, indexs: Array[Int], defaultVal: String): String = {
        def getUserDefinedValInner(index: Int): String = {
            if (index >= row.length || index < 0)
                return defaultVal
            "" + row.get(index)
        }

        var res = ""
        for (index <- indexs) {
            res += getUserDefinedValInner(index)
        }
        res
    }

    /**
      * 上层过滤数据
      * 目前只支持and的条件
      *
      * @param rowValue
      * @param transformerWhere
      * @return
      */
    def filterRow(rowValue: Row, transformerWhere: String, schema: ArrayBuffer[(String, DataType)]): Boolean = {
        def getFieldValAndLiterVal(str:String, splitter:String): (Boolean, String, String) = {

            def getFieldIndexInSchema(field: String): Int = {
                if (field.length > 0) {
                    for (index <- schema.indices) {
                        if (schema.get(index)._1.equals(field)) {
                            return index
                        }
                    }
                }
                -1
            }

            var res = (false, "","")
            val keyValue = str.split(splitter)
            if(keyValue.length==2) {
                val fieldIndex = getFieldIndexInSchema(keyValue.head)
                if(fieldIndex>(-1)) {
                    val fieldVal = rowValue.get(fieldIndex).toString
                    val literVal = trimFirstAndLastChar(keyValue.last, "'")
                    res = (true, fieldVal, literVal)
                }
            }
            res
        }

        val filter = false
        try {
            //只解析and
            val conditionParts = transformerWhere.split("and")
            for (conditionPart <- conditionParts) {
                conditionPart.trim match {
                    case s if s.contains("<>") => {
                        val res = getFieldValAndLiterVal(s, "<>")
                        if(res._1 && res._2.equals(res._3))
                            return true
                    }
                    case s if s.contains("=") => {
                        val res = getFieldValAndLiterVal(s, "=")
                        if(res._1 && !res._2.equals(res._3))
                            return true
                    }
                }
            }
        } catch {
            case e: Exception =>
        }
        filter
    }

    def trimFirstAndLastChar(str:String, element:String):String = {
        var res = str.replace("\"", "'")
        val beginIndex = if (str.indexOf(element) == 0) 1 else 0
        val endIndex = if (str.lastIndexOf(element) + 1 == str.length()) str.lastIndexOf(element) else str.length()
        res = str.substring(beginIndex, endIndex)
        res
    }
}
