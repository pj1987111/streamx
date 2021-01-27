package cn.tongdun.streamx.core.entity

import scala.beans.BeanProperty

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-07-07
  *  \* Time: 09:59
  *  \* Description: 
  *  \*/
case class SortConf() extends Serializable {
    /**
      * dml实时同步使用，分区key，主键，排序key
      */
    @BeanProperty
    var partitionKey: String = ""
    @BeanProperty
    var primaryKey: String = ""
    @BeanProperty
    var sortKey: String = ""

    var partitionKeyIndex: Array[Int] = _
    var primaryKeyIndex: Array[Int] = _
    var sortKeyIndex: Array[Int] = _
}
