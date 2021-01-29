package com.zhy.streamx.core

import com.zhy.streamx.core.entity.StreamxConf
import com.zhy.streamx.core.util.StreamUtils
import org.apache.commons.cli.{GnuParser, Option, Options}
import org.apache.spark.sql.SparkSession

/**
  * 传参数方式启动
  */
class StreamCollectorConfigStr extends StreamCollectorCore {

    /**
      * 命令行(base64/plain)转配置
      *
      * @param strings
      * @return
      */
    override def createParams(strings: Array[String]): StreamxConf = {
        val options = new Options
        val option = new Option("j", "json", true, "the json conf str")
        val option2 = new Option("l", "log", true, "log level")
        val option3 = new Option("e", "env", true, "env, pro or dev")
        val option4 = new Option("c", "json", true, "the json param str")
        val option5 = new Option("i", "idc", true, "idc")
        option.setRequired(true)
        options
                .addOption(option)
                .addOption(option2)
                .addOption(option3)
                .addOption(option4)
                .addOption(option5)
        val commandLine = new GnuParser().parse(options, strings)
        //json or base64 encode
        val confStr = commandLine.getOptionValue('j')
        StreamUtils.parseStreamxConf(confStr)
    }
}

object StreamCollectorConfigStr {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
                .builder()
//                .master("yarn")
                .appName("StreamCollectorConfigStr")
//                .enableHiveSupport()
                .getOrCreate()
        val stream = new StreamCollectorConfigStr()
        stream.runJob(spark, args)
    }
}