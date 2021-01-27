package cn.tongdun.streamx.redis.writer.enum

/**
  * @Author hongyi.zhou
  * @Description
  * @Date create in 2020-04-09. 
  **/
object OperationType extends Enumeration{
    val SET = Value(0,"set")
    val LPUSH = Value(2,"lpush")
    val SADD = Value(3,"sadd")

    val HSET = Value(1,"hset")
    val ZADD = Value(4,"zadd")

    /**
      * 是否存在
      * @param operation
      * @return
      */
    def checkExists(operation:String) = this.values.exists(_.toString.equals(operation))


}
