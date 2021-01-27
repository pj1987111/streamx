package cn.tongdun.streamx.core

import cn.tongdun.streamx.core.constants.Key
import cn.tongdun.streamx.core.entity.{ConfigConf, ExchangeDataConf, StreamxConf}
import cn.tongdun.streamx.core.plugins.PluginManager
import cn.tongdun.streamx.core.util.broadcast.BroadcastWrapperInstance
import cn.tongdun.streamx.core.util.{ConfUtil, StreamUtils}
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.codehaus.jackson.map.ObjectMapper

/**
  * @Author hongyi.zhou
  * @Description 核心处理
  * @Date create in 2020-02-26.
  **/
abstract class StreamCollectorCore extends Serializable {
    @transient private lazy val logger = LogManager.getLogger(classOf[StreamCollectorCore])

    def runJob(spark: SparkSession, strings: Array[String]): Unit = {
        val mapper = new ObjectMapper()
        logger.info(s"version 1.0.0_20210127")
        /**
          * 配置及广播初始化
          */
        val streamxConf = createParams(strings)
        val batchTime = streamxConf.getParams.getBatchDuration.toLong
        val configs = streamxConf.getConfigs
        val ssc = new StreamingContext(spark.sparkContext, Seconds(batchTime))
        initBroadcast(spark, configs)
        logger.info(s"streamxConf is : ${mapper.writeValueAsString(streamxConf)}")
        logger.info(s"batchTime secs is : $batchTime")

        /**
          * 插件启动
          */
        new PluginManager(ssc, spark, streamxConf.getPlugins, streamxConf).init().run()

        /**
          * 流交换线程启动
          */
        for (index <- configs.indices) {
            StreamCollectorCore.startNewExchange(spark, configs(index), ssc, index)
        }
        ssc.start
        ssc.awaitTermination
    }

    /**
      * 初始化广播配置
      *
      * @param spark
      * @param configs
      * @return
      */
    def initBroadcast(spark: SparkSession, configs: Array[ConfigConf]) = {
        val exchangeDataConfs = new Array[ExchangeDataConf](configs.length)
        for (index <- configs.indices) {
            exchangeDataConfs(index) = new ExchangeDataConf(spark, configs(index)).init()
        }
        BroadcastWrapperInstance.getInstance(spark.sparkContext, exchangeDataConfs)
    }

    /**
      * 配置初始化
      *
      * @param params
      * @return
      */
    def createParams(params: Array[String]): StreamxConf
}

object StreamCollectorCore {
    /**
      * 启动交换
      *
      * @param spark
      * @param config
      * @param ssc
      * @return
      */
    def startNewExchange(spark: SparkSession, config: ConfigConf,
                         ssc: StreamingContext, index: Int): String = {
        val ab = StreamUtils.initReader(config.getReader.get(Key.NAME).asInstanceOf[String],
            ssc, spark, config, ConfUtil.toStr(config.getReader))
        ab.setIndex(index)
        ab.read()
        ab.primaryKey
    }
}

