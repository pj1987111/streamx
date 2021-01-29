package com.zhy.streamx.addon.failover

import scala.beans.BeanProperty

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-05-27
  *  \* Time: 16:36
  *  \* Description: 
  *  \*/
case class FailoverPluginConf() {
    /**
      * 扫描间隔
      */
    @BeanProperty
    var sleepSec: Int = _
    /**
      * 入库异常文件存放根目录
      */
    @BeanProperty
    var failDir: String = _
}
