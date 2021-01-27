package cn.tongdun.streamx.core.api.plugin

import cn.tongdun.streamx.core.entity.StreamxConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-05-27
  *  \* Time: 16:38
  *  \* Description: 插件抽象类
  *  \*/
abstract class AbstractPlugin(ssc: StreamingContext, spark: SparkSession, streamxConf: StreamxConf) extends Serializable {
    /**
      * 插件类名全路径
      */
    var cName: String = _

    /**
      * 初始化
      *
      * @param cName
      * @param pluginConfigStr
      */
    def init(cName: String, pluginConfigStr: String): Unit

    /**
      * 执行
      */
    def run(): Unit
}
