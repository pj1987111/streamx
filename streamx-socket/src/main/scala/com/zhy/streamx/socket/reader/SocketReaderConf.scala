package com.zhy.streamx.socket.reader

import scala.beans.BeanProperty

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2021-01-29
  *  \* Time: 16:38
  *  \* Description: 
  *  \*/
case class SocketReaderConf() extends Serializable {
    //插件名称
    @BeanProperty
    var name: String = _
    @BeanProperty
    var ip: String = _
    @BeanProperty
    var port: String = _
}
