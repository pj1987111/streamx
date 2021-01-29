package com.zhy.streamx.core.entity

import scala.beans.BeanProperty

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-05-29
  *  \* Time: 14:13
  *  \* Description: 全局配置
  *  \*/
case class ParamsConf() {
    /**
      * spark streaming消费间隔(s)
      */
    @BeanProperty
    var batchDuration: String = _

    override def toString = s"ParamsConf($batchDuration)"
}
