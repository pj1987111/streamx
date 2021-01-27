package cn.tongdun.streamx.mysql.writer

import scala.beans.BeanProperty

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-06-17
  *  \* Time: 18:38
  *  \* Description: 
  *  \*/
class MysqlConf {
    //mysql url
    @BeanProperty
    var url: String = ""
    //mysql user
    @BeanProperty
    var user: String = ""
    //mysql password
    @BeanProperty
    var password: String = ""
    //mysql table
    @BeanProperty
    var table: String = ""
    //批量提交个数
    @BeanProperty
    var batchSize: Int = 1000
}
