package com.zhy.streamx.kafka.reader.util

import java.util

import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.LogManager
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, OffsetRange}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}

import scala.collection.mutable.ArrayBuffer

/**
  * @Author hongyi.zhou
  * @Description 提交监听器，处理提交断点
  * @Date create in 2020-01-11.
  **/
class BatchStreamingListener(streams: InputDStream[ConsumerRecord[String, String]]) extends StreamingListener {
    @transient private lazy val logger = LogManager.getLogger(classOf[BatchStreamingListener])

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
        val errorTopic = ArrayBuffer.empty[Integer]
        val outputOperationInfos = batchCompleted.batchInfo.outputOperationInfos
        val streamIdToInputInfo = batchCompleted.batchInfo.streamIdToInputInfo
        //检查每个topic是否成功
        outputOperationInfos.foreach(tuple => {
            val outputInfo = tuple._2
            if (outputInfo.failureReason.isDefined) {
                logger.info(s"id[${outputInfo.id}] has failureReason : ${outputInfo.failureReason}")
                errorTopic += tuple._1
            }
        })
        //写入断点
        streamIdToInputInfo.foreach(tuple => {
            if (!errorTopic.contains(tuple._1)) {
                val offsetRangesL = tuple._2.metadata("offsets").asInstanceOf[List[OffsetRange]]
                val offsetRanges = offsetRangesL.toArray
                streams.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges, new OffsetCommitCallback() {
                    override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], e: Exception): Unit = {
                        if (null != e) {
                            streams.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
                        }
                    }
                })
                //        sparkJobLogger.info(s"id[${tuple._1}] end onBatchCompleted commit offsetRanges $offsetRangesL")
            }
        })
    }
}
