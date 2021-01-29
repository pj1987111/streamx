package com.zhy.streamx.core.entity

import java.util

import com.zhy.streamx.core.api.writer.AbstractWriter

import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-05-29
  *  \* Time: 14:12
  *  \* Description: 交换配置
  *  \*/
case class ConfigConf() extends Serializable {
    //配置文件中使用列
    /**
      * 源配置
      */
    @BeanProperty
    var reader: util.HashMap[String, Any] = _
    /**
      * 目标配置
      */
    @BeanProperty
    var writers: Array[util.HashMap[String, Any]] = _
    /**
      * 转换配置
      */
    @BeanProperty
    var transformer: TransformerConf = _

    //新增程序使用列
    /**
      * 对应目标库
      */
    var writerBuffer: ArrayBuffer[AbstractWriter] = ArrayBuffer.empty[AbstractWriter]
    /**
      * 读取临时表
      */
    var tblTable: String = _

    override def toString = s"ConfigConf($reader, $writers, $transformer)"
}
