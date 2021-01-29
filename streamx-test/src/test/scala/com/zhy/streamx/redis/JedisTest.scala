package com.zhy.streamx.redis

import com.zhy.streamx.redis.writer.util.RedisClient
import org.junit.Test
import redis.clients.jedis.Pipeline

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-06-30
  *  \* Time: 14:02
  *  \* Description: 
  *  \*/
class JedisTest {
    @Test
    def testJedis(): Unit = {
        val redisClient: RedisClient = RedisClient
                .builder()
                .withHost("10.57.22.115")
                .withJedisPort(6379)
                .withPassword("abc123")
                .withJedisDb(4)
        val jedis = RedisClient.buildConnection(redisClient)
        val key = "key112"
        val pipeline: Pipeline = jedis.pipelined
        pipeline.set(key, "{\"latitude\":\"29.207429401408184\",\"time\":\"2020-01-14 21:12:02\",\"userId\":\"85fedf45-ebaa-11e8-b9e4-fa163e800762\",\"longitude\":\"119.83009850567478\",\"sendTime\":\"2020-03-19 12:03:47\"}")
        pipeline.expire(key, 20)
        pipeline.sync()
        print(jedis)
    }
}
