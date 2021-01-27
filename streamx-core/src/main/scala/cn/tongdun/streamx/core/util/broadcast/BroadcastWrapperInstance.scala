package cn.tongdun.streamx.core.util.broadcast

import cn.tongdun.streamx.core.entity.ExchangeDataConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-06-01
  *  \* Time: 22:27
  *  \* Description: 广播变量支持更新
  *  \*/
object BroadcastWrapperInstance {

    @volatile private var instance: Broadcast[Array[ExchangeDataConf]] = _

    def getInstance(sc: SparkContext, newVal: Array[ExchangeDataConf] = null): Broadcast[Array[ExchangeDataConf]]={
        if (instance == null) {
            synchronized {
                if (instance == null) {
                    instance = sc.broadcast(newVal)
                }
            }
        }
        instance
    }

    def update(sc: SparkContext, newVal: Array[ExchangeDataConf], blocking: Boolean = false): Unit = {
        if (instance != null)
            instance.unpersist(blocking)
        instance = sc.broadcast(newVal)
    }
}
