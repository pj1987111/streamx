package cn.tongdun.streamx.core.plugins

import java.util

import cn.tongdun.streamx.core.api.plugin.{AbstractFailoverPlugin, AbstractPlugin}
import cn.tongdun.streamx.core.constants.Key
import cn.tongdun.streamx.core.entity.StreamxConf
import cn.tongdun.streamx.core.util.{ConfUtil, StreamUtils}
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable.ArrayBuffer

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-05-27
  *  \* Time: 16:30
  *  \* Description: 插件管理器
  *  \*/
class PluginManager(ssc: StreamingContext, spark: SparkSession, pluginConfigs: Array[util.HashMap[String, Any]], streamxConf: StreamxConf) {
    private val logger = LogManager.getLogger(classOf[PluginManager])

    val pluginBuffer: ArrayBuffer[AbstractPlugin] = ArrayBuffer.empty[AbstractPlugin]

    def init(): PluginManager = {
        if (pluginConfigs != null && pluginConfigs.nonEmpty) {
            for (plugin <- pluginConfigs) {
                pluginBuffer += StreamUtils.initPlugin(
                    plugin.get(Key.NAME).asInstanceOf[String], ssc,
                    spark, ConfUtil.toStr(plugin), streamxConf)
            }
        }
        PluginManager.getFailoverInstance(pluginBuffer)
        this
    }

    def run(): Unit = {
        for (pb <- pluginBuffer) {
            try {
                pb.run()
            } catch {
                case e: Exception => {
                    logger.error(s"error ${e.getMessage}", e)
                    logger.error(s"error ${e.getMessage}", e)
                }
            }
        }
    }
}

object PluginManager {
    @volatile private var failover_instance: ArrayBuffer[AbstractPlugin] = _

    def getFailoverInstance(vals: ArrayBuffer[AbstractPlugin] = null): ArrayBuffer[AbstractPlugin] = {
        if (failover_instance == null) {
            synchronized {
                if (failover_instance == null) {
                    failover_instance = ArrayBuffer.empty[AbstractPlugin]
                    for(valv <- vals) {
                        if(valv.isInstanceOf[AbstractFailoverPlugin]) {
                            failover_instance += valv
                        }
                    }
                }
            }
        }
        failover_instance
    }
}
