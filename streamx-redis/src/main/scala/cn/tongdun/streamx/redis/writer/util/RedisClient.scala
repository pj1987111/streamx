package cn.tongdun.streamx.redis.writer.util

import java.{lang, util}

import cn.tongdun.streamx.redis.writer.enum.OperationType
import cn.tongdun.streamx.redis.writer.exception.RedisWriterException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.codehaus.jackson.map.ObjectMapper
import redis.clients.jedis._

/**
  * @Author hongyi.zhou
  * @Description
  * @Date create in 2020-04-08. 
  **/
case class RedisClient() extends Serializable {
    private var host: String = _
    private var password: String = _
    private var jedisPort: Int = Protocol.DEFAULT_PORT
    private var jedisDb: Int = Protocol.DEFAULT_DATABASE

    def withHost(host: String): RedisClient = {
        this.host = host
        this
    }

    def withPassword(password: String): RedisClient = {
        this.password = password
        this
    }

    def withJedisPort(jedisPort: Int): RedisClient = {
        this.jedisPort = jedisPort
        this
    }

    def withJedisDb(jedisDb: Int): RedisClient = {
        this.jedisDb = jedisDb
        this
    }
}


object RedisClient {

    /** ********REDIS客户端类型 ********/
    val JEDIS: String = "JEDIS"
    val COIDS: String = "CODIS"

    /** ********JEDIS基本配置 ********/
    val JEDIS_MAX_TOTAL: Int = 500
    val JEDIS_MAX_IDEL: Int = 5
    val JEDIS_MAX_WAIT_MillIS: Int = 1000 * 10
    val JEDIS_TEST_ON_BORROW: Boolean = true
    val JEDIS_TIME_OUT: Int = 30000

    /** ********CODIS基本配置 ********/
    val CODIS_SESSION_TIMEOUT: Int = 15000
    val CODIS_CONNECT_TIMEOUT: Int = 1000

    def builder(): RedisClient = {
        new RedisClient
    }

    def buildConnection(redisClient: RedisClient): Jedis = {
        getConnectionByJedis(redisClient)
    }

    private def getConnectionByJedis(redisClient: RedisClient): Jedis = {
        val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig
        jedisPoolConfig.setMaxTotal(JEDIS_MAX_TOTAL)
        jedisPoolConfig.setMaxIdle(JEDIS_MAX_IDEL)
        jedisPoolConfig.setMaxWaitMillis(JEDIS_MAX_WAIT_MillIS)
        jedisPoolConfig.setTestOnBorrow(JEDIS_TEST_ON_BORROW)
        val jedis: Jedis = new JedisPool(jedisPoolConfig, redisClient.host, redisClient.jedisPort, JEDIS_TIME_OUT, redisClient.password, redisClient.jedisDb).getResource
        jedis.ping()
        jedis
    }

    def partitionPipeOper(jedis: Jedis,
                          iterator: Iterator[Row],
                          schemaList: List[StructField],
                          operation: String,
                          prefixKey: String,
                          batchSize: Int,
                          expireTime: Int): Unit = {
        val pipeline: Pipeline = jedis.pipelined
        var count = 0
        iterator.foreach(row => {
            var redisKey = prefixKey
            if (!row.isNullAt(0)) {
                redisKey = redisKey + row.getAs[String](0)
            }
            //set lpush sadd 支持插入json
            operation match {
                //set 值
                case oper if OperationType.SET.toString.equals(oper) => {
                    val value = getVal(row, schemaList)
                    pipeSetOper(pipeline, redisKey, value)
                }
                //插入List
                case oper if OperationType.LPUSH.toString.equals(oper) => {
                    val value = getVal(row, schemaList)
                    pipeLpushOper(pipeline, redisKey, value)
                }
                //插入Set
                case oper if OperationType.SADD.toString.equals(oper) => {
                    val value = getVal(row, schemaList)
                    pipeSaddOper(pipeline, redisKey, value)
                }
                //指定列名set
                case oper if OperationType.HSET.toString.equals(oper) => {
                    val field = if (!row.isNullAt(1)) row.get(1).toString else ""
                    val value = if (!row.isNullAt(2)) row.get(2).toString else ""
                    pipeHsetOper(pipeline, redisKey, field, value)
                }
                //有序集合 分数大的覆盖小的
                case oper if OperationType.ZADD.toString.equals(oper) => {
                    if (!row.isNullAt(1)) {
                        val score = row.get(1).toString.toDouble
                        val value = if (!row.isNullAt(2)) row.get(2).toString else ""
                        pipeZaddOper(pipeline, redisKey, score, value)
                    }
                }
                case _ => throw new RedisWriterException("操作类型错误:" + operation)
            }
            if (0 != expireTime) {
                pipeline.expire(redisKey, expireTime)
            }
            count += 1
            if (count % batchSize == 0) {
                pipeline.sync()
                count = 0
            }
        })
        if (count > 0) {
            pipeline.sync()
        }
    }

    private def getVal(row: Row, schemaList: List[StructField]): String = {
        var value = ""
        //多于两列自动变json
        if (row.length > 2) {
            val map = new util.HashMap[String, Object]
            for (index <- 1 until row.length) {
                if (row.get(index) != null) {
                    getObjVal(row, index, schemaList.apply(index), map)
                }
            }
            value = new ObjectMapper().writeValueAsString(map)
        }
        //两列key-value
        else {
            value = if (!row.isNullAt(1)) row.get(1).toString else ""
        }
        value
    }

    private def getObjVal(r: Row, index: Int, sField: StructField, map: util.HashMap[String, Object]) = {
        sField.dataType match {
            case IntegerType => map.put(sField.name, new Integer(r.getInt(index)))
            case LongType => map.put(sField.name, new java.lang.Long(r.getLong(index)))
            case DoubleType => map.put(sField.name, new lang.Double(r.getDouble(index)))
            case ShortType => map.put(sField.name, new java.lang.Short(r.getShort(index)))
            case FloatType => map.put(sField.name, new java.lang.Float(r.getFloat(index)))
            case BooleanType => map.put(sField.name, new java.lang.Boolean(r.getBoolean(index)))
            case _ => map.put(sField.name, r.get(index).toString)
        }
    }

    private def pipeSetOper(pipeline: Pipeline, key: String, value: String): Unit = {
        pipeline.set(key, value)
    }

    private def pipeHsetOper(pipeline: Pipeline, key: String, field: String, value: String): Unit = {
        pipeline.hset(key, field, value)
    }

    private def pipeLpushOper(pipeline: Pipeline, key: String, value: String): Unit = {
        pipeline.lpush(key, value)
    }

    private def pipeSaddOper(pipeline: Pipeline, key: String, value: String): Unit = {
        pipeline.sadd(key, value)
    }

    private def pipeZaddOper(pipeline: Pipeline, key: String, score: Double, value: String): Unit = {
        pipeline.zadd(key, score, value)
    }


}
