package cn.tongdun.streamx.core

import cn.tongdun.streamx.core.config.ConfigParser
import cn.tongdun.streamx.core.entity.StreamxConf
import cn.tongdun.streamx.core.util.StreamUtils
import org.apache.spark.sql.SparkSession

/**
  * 配置文件启动
  */
class StreamCollectorConfigFile extends StreamCollectorCore {

    /**
      * 配置文件(local/hdfs)转配置
      *
      * @param strings
      * @return
      */
    override def createParams(strings: Array[String]): StreamxConf = {
        if (strings == null || strings.length != 1) {
            throw new RuntimeException("参数输入错误,文件路径")
        }
        ConfigParser.reload(StreamUtils.readConfig(strings(0)))
    }
}

object StreamCollectorConfigFile {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
                .builder()
                .master("yarn")
                .appName("StreamCollectorConfigFile")
                .enableHiveSupport()
                .getOrCreate()
        val stream = new StreamCollectorConfigFile()
        stream.runJob(spark, args)
    }
}