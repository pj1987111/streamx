package cn.tongdun.streamx.redis.writer

import scala.beans.BeanProperty

/**
  * @Author hongyi.zhou
  * @Description
  * @Date create in 2020-04-14. 
  **/
case class RedisConf() {
    //redis host
    @BeanProperty
    var host: String = ""
    //redis port
    @BeanProperty
    var port: Int = 6379
    //redis password
    @BeanProperty
    var password: String = ""
    //redis db
    @BeanProperty
    var database: Int = 0
    //type 支持set lpush sadd hset zadd
    @BeanProperty
    var operation: String = ""
    //redis key前缀
    @BeanProperty
    var prefix: String = ""
    //批量提交个数
    @BeanProperty
    var batchSize: Int = 1000
    //key 过期时间
    @BeanProperty
    var expireTime: Int = 0
}
