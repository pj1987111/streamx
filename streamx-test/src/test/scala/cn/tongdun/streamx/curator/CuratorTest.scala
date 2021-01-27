package cn.tongdun.streamx.curator

import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode
import org.junit.{Before, Test}

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-05-28
  *  \* Time: 14:14
  *  \* Description: 
  *  \*/
class CuratorTest {
    var client: CuratorFramework = _

    @Before
    def setUp() = {
        val retryPolicy = new ExponentialBackoffRetry(1000, 3)
        client = CuratorFrameworkFactory.builder()
                .connectString("spark-p-006068.hhy.td:2181,spark-p-006072.hhy.td:2181," +
                        "spark-p-006078.hhy.td:2181,spark-p-006082.hhy.td:2181,spark-p-006088.hhy.td:2181")
                .sessionTimeoutMs(30000)
                .connectionTimeoutMs(50000)
                .retryPolicy(retryPolicy)
                .build()
    }

    @Test
    def simpleTest(): Unit = {
        /**
          * 创建会话
          **/
        client.start()
        //空节点，目录递归创建
        client.create.creatingParentsIfNeeded.forPath("/zhytest/1")
        //有内容的节点
        client.create.creatingParentsIfNeeded.forPath("/zhytest/2", "zhy".getBytes)
        client.create.creatingParentsIfNeeded.forPath("/zhytest/3", "zhytest123456".getBytes)
        client.close()
    }

    @Test
    def check(): Unit ={
        client.start()
        val stat = client.checkExists().forPath("/123hbasd")
        println(stat)
    }
    @Test
    def simpleTest2(): Unit = {
        /**
          * 创建会话
          **/
        client.start()
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                .forPath("/zhytest/app1")
//                .forPath("/zhytest1/1/temp1", "zhy123123".getBytes())
        while (true) {
            Thread.sleep(10 * 1000)
        }
        client.close()
    }

    @Test
    def editTest(): Unit = {
        client.start()
        val stat = client.setData().forPath("/zhytest/1/temp1")
        client.setData().forPath("/zhytest/1/temp1", "99999".getBytes)
        client.close()
    }

    @Test
    def nodeCacheTest(): Unit = {
        client.start
        //        client.getData.usingWatcher(new Watcher() {
        //            override def process(watchedEvent: WatchedEvent): Unit = {
        //                System.out.println("监听器watchedEvent：" + watchedEvent)
        //            }
        //        }).forPath("/zhytest/1/temp1")
        //        while(true) {
        //            Thread.sleep(10*1000)
        //        }
        //        client.close

        val cache = new NodeCache(client, "/zhytest/1/temp1")
        val listener = new NodeCacheListener() {
            override def nodeChanged(): Unit = {
                val data = cache.getCurrentData
                if (null != data) {
                    println("节点数据：" + new String(cache.getCurrentData.getData))
                }
                else {
                    println("节点被删除!")
                }
            }
        }
        cache.getListenable.addListener(listener)
        cache.start()
        while (true) {
            Thread.sleep(10 * 1000)
        }
        client.close
    }

    @Test
    def changeNodeValue(): Unit = {
        val str =
            """
              |{
              |  "params": {
              |    "batchDuration": "10"
              |  },
              |  "plugins": [
              |    {
              |      "name": "cn.tongdun.streamx.addon.failover.FailoverPlugin",
              |      "sleepSec": "180",
              |      "failDir": "/user/backup/app1"
              |    },
              |    {
              |      "name": "cn.tongdun.streamx.addon.discovery.ZkDiscoveryPlugin",
              |      "zkAddress": "spark-p-006068.hhy.td:2181,spark-p-006072.hhy.td:2181,spark-p-006078.hhy.td:2181,spark-p-006082.hhy.td:2181,spark-p-006088.hhy.td:2181",
              |      "zkNodePath": "/zhytest/app1"
              |    }
              |  ],
              |  "configs": [
              |    {
              |      "reader": {
              |        "name": "cn.tongdun.streamx.kafka.reader.KafkaReader",
              |        "brokerList": "33.69.6.13:9092,33.69.6.14:9092,33.69.6.15:9092,33.69.6.16:9092,33.69.6.17:9092,33.69.6.18:9092,33.69.6.19:9092,33.69.6.20:9092,33.69.6.21:9092,33.69.6.22:9092",
              |        "topic": "test_zhy2",
              |        "groupId": "20200525_1",
              |        "extraConfigs": "",
              |        "parser": "json"
              |      },
              |      "writers": [
              |        {
              |          "name": "cn.tongdun.streamx.elastic.writer.ElasticWriter",
              |          "url": "33.69.6.98:9221",
              |          "index": "zhy_test_job1_4",
              |          "partitionValue": "",
              |          "type": "job4",
              |          "batchSize": 200,
              |          "hasId": false
              |        },
              |        {
              |          "name": "cn.tongdun.streamx.hive.writer.HiveWriter",
              |          "tableName": "hhy_dw.test_zhy1_tab_dt",
              |          "partitionValue": "ds",
              |          "hiveSetConfigs": ""
              |        }
              |      ],
              |      "transformer": {
              |        "where": "",
              |        "reader_fields": {
              |          "id": {
              |            "type": "string"
              |          },
              |          "latitude": {
              |            "type": "string"
              |          },
              |          "longitude": {
              |            "type": "string"
              |          },
              |          "time": {
              |            "type": "string"
              |          },
              |          "sendTime": {
              |            "type": "string"
              |          }
              |        },
              |        "writer_fields": [
              |          "*",
              |          "substr(time,0,10) as inputTime"
              |        ]
              |      }
              |    }
              |  ]
              |}
            """.stripMargin
        client.start()
        client.setData().forPath("/zhytest/app1", str.getBytes)
        client.close()
    }
}
