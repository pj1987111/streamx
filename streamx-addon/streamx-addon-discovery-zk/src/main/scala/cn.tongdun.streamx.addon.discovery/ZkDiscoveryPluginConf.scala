package cn.tongdun.streamx.addon.discovery

import scala.beans.BeanProperty

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-05-28
  *  \* Time: 11:07
  *  \* Description: 
  *  \*/
case class ZkDiscoveryPluginConf() {
    /**
      * zk地址
      */
    @BeanProperty
    var zkAddress: String = _
    /**
      * zk临时节点路径
      */
    @BeanProperty
    var zkNodePath: String = _
}
