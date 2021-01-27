package cn.tongdun.streamx.redis.writer

import cn.tongdun.streamx.core.api.writer.AbstractWriter
import cn.tongdun.streamx.core.entity.ConfigConf
import cn.tongdun.streamx.core.util.ConfUtil
import cn.tongdun.streamx.redis.writer.util.RedisClient
import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.Jedis

/**
  * @Author hongyi.zhou
  * @Description
  * @Date create in 2020-04-06. 
  **/
class RedisWriter(spark: SparkSession, configConf: ConfigConf) extends AbstractWriter(spark, configConf) {
    var name = ""
    var host = ""
    var port = 0
    var password = ""
    var database = 0
    var operation = ""
    var prefix = ""
    var batchSize = 1000
    var expireTime = 0

    override def init(cName: String, writerConfigStr: String): Unit = {
        this.cName = cName

        val writerConf = ConfUtil.getWholeConf(writerConfigStr, classOf[RedisConf])
        host = writerConf.getHost
        port = writerConf.getPort
        password = writerConf.getPassword
        database = writerConf.getDatabase
        operation = writerConf.getOperation
        prefix = writerConf.getPrefix
        batchSize = writerConf.getBatchSize
        expireTime = writerConf.getExpireTime
    }

    override def write(df: DataFrame): Unit = {
        createView(df)
        val dfT = spark.sql(selectTmpSql())
        val schemaList = dfT.schema.toList
        val redisClient: RedisClient = RedisClient
                .builder()
                .withHost(host)
                .withJedisPort(port)
                .withPassword(password)
                .withJedisDb(database)
        dfT.foreachPartition(rows => {
            if(rows.nonEmpty) {
                var jedis: Jedis = null
                try {
                    jedis = RedisClient.buildConnection(redisClient)
                    RedisClient.partitionPipeOper(jedis, rows, schemaList, operation, prefix, batchSize, expireTime)
                } finally {
                    if (null != jedis) jedis.close()
                }
            }
        })
    }
}
