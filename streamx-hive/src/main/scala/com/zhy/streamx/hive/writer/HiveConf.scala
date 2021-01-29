package com.zhy.streamx.hive.writer

import java.util

import scala.beans.BeanProperty

/**
  * @Author hongyi.zhou
  * @Description
  * @Date create in 2020-04-14.
  **/
case class HiveConf() {
    //写入hive表名
    @BeanProperty
    var tableName: String = ""
    //写入hive分区字段，支持动态分区(ds)，静态分区(ds=20)以及不写表示没分区
    @BeanProperty
    var partitionValue: String = ""
    //专用hive配置
    @BeanProperty
    var hiveSetConfigs: String = ""
    //跨集群配置
    @BeanProperty
    var clusterConfig: util.HashMap[String, String] = _
}
