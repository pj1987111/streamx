package cn.tongdun.streamx.addon.discovery

import java.util.concurrent.ThreadPoolExecutor

import cn.tongdun.streamx.core.StreamCollectorCore
import cn.tongdun.streamx.core.api.plugin.AbstractPlugin
import cn.tongdun.streamx.core.constants.Key
import cn.tongdun.streamx.core.entity.{ConfigConf, ExchangeDataConf, StreamxConf}
import cn.tongdun.streamx.core.util.broadcast.BroadcastWrapperInstance
import cn.tongdun.streamx.core.util.{ConfUtil, StreamUtils, ThreadUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.zookeeper.CreateMode

import scala.collection.mutable.ArrayBuffer

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-05-23
  *  \* Time: 16:52
  *  \* Description: 基于zk的元数据感知插件
  *  \*/
class ZkDiscoveryPlugin(ssc: StreamingContext, spark: SparkSession, streamxConf: StreamxConf)
        extends AbstractPlugin(ssc, spark, streamxConf) {
    @transient private lazy val logger = LogManager.getLogger(classOf[ZkDiscoveryPlugin])

    var client: CuratorFramework = _
    var zkAddress: String = ""
    var zkNodePath: String = ""
    var threadPool: ThreadPoolExecutor = _
    var listenerArray: ArrayBuffer[String] = ArrayBuffer.empty[String]

    /**
      * 初始化
      *
      * @param cName
      * @param pluginConfigStr
      */
    override def init(cName: String, pluginConfigStr: String): Unit = {
        this.cName = cName

        val pluginConf = ConfUtil.getWholeConf(pluginConfigStr, classOf[ZkDiscoveryPluginConf])
        zkAddress = pluginConf.getZkAddress
        zkNodePath = pluginConf.getZkNodePath
        if (StringUtils.isBlank(zkAddress) || StringUtils.isBlank(zkNodePath)) {
            logger.warn("zkAddress or zkNodePath is not set,this plugin is not start")
            return
        }
        val retryPolicy = new ExponentialBackoffRetry(1000, 3)
        client = CuratorFrameworkFactory.builder()
                .connectString(zkAddress)
                .sessionTimeoutMs(30000)
                .connectionTimeoutMs(50000)
                .retryPolicy(retryPolicy)
                .build()
        client.start()
        threadPool = ThreadUtils.newDaemonFixedThreadPool(1, "ZkDiscoveryPlugin")
        createNode()
        registerListener()
        streamxConf.getConfigs.foreach(config => {
            val ab = StreamUtils.initReader(config.getReader.get(Key.NAME).asInstanceOf[String],
                ssc, spark, config, ConfUtil.toStr(config.getReader))
            listenerArray += ab.primaryKey
        })
    }

    /**
      * 临时节点初始化
      */
    def createNode(): Unit = {
        if (client.checkExists().forPath(zkNodePath) != null) {
            client.delete().deletingChildrenIfNeeded().forPath(zkNodePath)
        }
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(zkNodePath, "".getBytes)
    }

    /**
      * 会话监听器
      * 防止zk因为网络抖动断连
      */
    def registerListener(): Unit = {
        client.getConnectionStateListenable.addListener(new SessionConnectionListener(zkNodePath, ""))
    }

    /**
      * 监听节点
      * 实时变更配置
      */
    def watch(): Unit = {
        val cache = new NodeCache(client, zkNodePath)
        val listener = new NodeCacheListener() {
            override def nodeChanged(): Unit = {
                val data = cache.getCurrentData
                if (null != data && data.getData.length > 0) {
                    logger.info(s"node $zkNodePath has been edited,refresh...")
                    val newStreamxConf = parseConfig(new String(data.getData))
                    if (newStreamxConf != null) {
                        //重新发布广播变量
                        reBroadcast(spark, newStreamxConf.configs)
                        //新增的配置直接启动
                        for (index <- newStreamxConf.getConfigs.indices) {
                            val config = newStreamxConf.getConfigs.apply(index)
                            val ab = StreamUtils.initReader(config.getReader.get(Key.NAME).asInstanceOf[String],
                                ssc, spark, config, ConfUtil.toStr(config.getReader))
                            //添加
                            if (!listenerArray.contains(ab.primaryKey)) {
                                listenerArray += StreamCollectorCore.startNewExchange(spark,
                                    config, ssc, index)
                            }
                        }
                    }
                }
                else {
                    logger.info(s"node $zkNodePath has been delete")
                }
            }
        }
        cache.getListenable.addListener(listener)
        cache.start()
    }

    def parseConfig(dataStr: String): StreamxConf = {
        var streamxConf: StreamxConf = null
        try {
            streamxConf = StreamUtils.parseStreamxConf(dataStr)
        } catch {
            case e: Exception => {
                logger.error("streamxConf is error", e)
            }
        }
        streamxConf
    }

    def reBroadcast(spark: SparkSession, configs: Array[ConfigConf]) = {
        val exchangeDataConfs = new Array[ExchangeDataConf](configs.length)
        for (index <- configs.indices) {
            exchangeDataConfs(index) = new ExchangeDataConf(spark, configs(index)).init()
        }
        BroadcastWrapperInstance.update(spark.sparkContext, exchangeDataConfs)
    }

    /**
      * 执行
      */
    override def run(): Unit = {
        threadPool.submit(new Runnable {
            override def run(): Unit = {
                watch()
            }
        })
    }
}