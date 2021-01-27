package cn.tongdun.streamx.kafka.reader.util

import org.apache.kafka.clients.consumer.{Consumer, KafkaConsumer, NoOffsetForPartitionException}
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Kafka辅助处理工具
  */
object KafkaOffsetUtils {

    private val logger = LoggerFactory.getLogger(this.getClass)

    /**
      * 获取每个分区的最小offset
      *
      * @param consumer   消费者
      * @param partitions topic分区
      * @return
      */
    def getEarliestOffsets(consumer: Consumer[_, _], partitions: Set[TopicPartition]): Map[TopicPartition, Long] = {
        consumer.seekToBeginning(partitions.asJava)
        partitions.map(tp => tp -> consumer.position(tp)).toMap
    }


    /**
      * 获取当前的offsets
      *
      * @param kafkaParams kafka参数
      * @param topics      topic
      * @return
      */
    def getCurrentOffset(kafkaParams: Map[String, Object], topics: Set[String]): Map[TopicPartition, Long] = {
        val consumer: KafkaConsumer[String, Array[Byte]] = new KafkaConsumer[String, Array[Byte]](kafkaParams.asJava)
        consumer.subscribe(topics.asJava)
        val notOffsetTopicPartition = mutable.Set[TopicPartition]()
        try {
            consumer.poll(0)
        } catch {
            case ex: NoOffsetForPartitionException =>
                logger.warn(s"consumer topic partition offset not found:${ex.partitions()}")
                for (part <- ex.partitions()) {
                    notOffsetTopicPartition.add(part)
                }
        }
        val parts = consumer.assignment().asScala
        consumer.pause(parts.asJava)
        val topicPartition = parts.diff(notOffsetTopicPartition)
        //获取当前offset
        val currentOffset = mutable.Map[TopicPartition, Long]()
        topicPartition.foreach(x => {
            try {
                currentOffset.put(x, consumer.position(x))
            } catch {
                case ex: NoOffsetForPartitionException =>
                    logger.warn(s"consumer topic partition offset not found:${ex.partitions()}")
            }
        })
        //获取earliestOffset
        val earliestOffset = getEarliestOffsets(consumer, parts.toSet)
        earliestOffset.foreach(x => {
            val value = currentOffset.get(x._1)
            //判断当前消费的offset和分区最早的offset的大小
            if (value.isEmpty) {
                currentOffset(x._1) = x._2
            } else if (value.get < x._2) {
                logger.warn(s"kafka data is lost from partition:${x._1} offset ${value.get} to ${x._2}")
                currentOffset(x._1) = x._2
            }
        })
        consumer.unsubscribe()
        consumer.close()
        currentOffset.toMap
    }
}
