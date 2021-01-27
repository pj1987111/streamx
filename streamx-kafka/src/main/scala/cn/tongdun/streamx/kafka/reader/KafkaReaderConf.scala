package cn.tongdun.streamx.kafka.reader

import scala.beans.BeanProperty

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-05-29
  *  \* Time: 14:14
  *  \* Description: kafka读配置
  *  \*/
case class KafkaReaderConf() extends Serializable {
    //插件名称
    @BeanProperty
    var name: String = _
    @BeanProperty
    var brokerList: String = _
    //kafka读取topic
    @BeanProperty
    var topic: String = _
    //kafka读取groupid
    @BeanProperty
    var groupId: String = _
    //kafka额外参数
    @BeanProperty
    var extraConfigs: String = _
    //数据解析器
    @BeanProperty
    var parser: String = _

    //临时视图名
    var tdlTableName: String = _

    override def toString = s"KafkaReaderConf($name, $brokerList, $topic, $groupId, $extraConfigs, $parser, $tdlTableName)"
}
