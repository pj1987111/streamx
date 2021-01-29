package com.zhy.streamx.core.entity

import java.util

import scala.beans.BeanProperty

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-05-29
  *  \* Time: 14:16
  *  \* Description: 主配置
  *  \*/
case class StreamxConf() {
    /**
      * 全局配置
      */
    @BeanProperty
    var params: ParamsConf = _
    /**
      * 插件配置
      */
    @BeanProperty
    var plugins: Array[util.HashMap[String, Any]] = _
    /**
      * 交换配置
      */
    @BeanProperty
    var configs: Array[ConfigConf] = _

    override def toString = s"StreamxConf($params, $plugins, $configs)"
}
