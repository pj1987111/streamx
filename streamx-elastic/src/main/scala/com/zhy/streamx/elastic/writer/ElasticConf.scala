package com.zhy.streamx.elastic.writer

import scala.beans.BeanProperty

/**
  * @Author hongyi.zhou
  * @Description
  * @Date create in 2020-04-14. 
  **/
case class ElasticConf() {
    //elastic url
    @BeanProperty
    var url: String = ""
    @BeanProperty
    var port: String = ""
    //elastic index
    @BeanProperty
    var index: String = ""
    //elastic type
    @BeanProperty
    var `type`: String = ""
    //批量提交个数
    @BeanProperty
    var batchSize: Int = 1000
    //是否含有id，true的话用第一列作为id，false的话系统默认生成一个uuid
    @BeanProperty
    var hasId: Boolean = false
    //elastic分区字段，多个用逗号分隔，需要和index中占位符数量对应
    @BeanProperty
    var partitionValue: String = ""
}
