package com.zhy.streamx.core.api.reader

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-06-03
  *  \* Time: 18:04
  *  \* Description: 支持不同源的写操作
  *  \*/
trait RowStream[T] extends Serializable {
    /**
      * 使用不同源的数据解析通用接口
      *
      * @param row
      * @return
      */
    def rowValue(row: T): String
}
