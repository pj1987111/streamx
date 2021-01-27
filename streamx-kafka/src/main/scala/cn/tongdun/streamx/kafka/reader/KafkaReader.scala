package cn.tongdun.streamx.kafka.reader

import cn.tongdun.streamx.core.ExchangeRunner
import cn.tongdun.streamx.core.api.reader.{AbstractReader, RowStream}
import cn.tongdun.streamx.core.constants.Key
import cn.tongdun.streamx.core.entity.ConfigConf
import cn.tongdun.streamx.core.util.ConfUtil
import cn.tongdun.streamx.kafka.reader.util.BatchStreamingListener
import org.apache.commons.lang.StringUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-06-03
  *  \* Time: 21:41
  *  \* Description: 
  *  \*/
class KafkaReader(ssc: StreamingContext, spark: SparkSession, configConf: ConfigConf)
        extends AbstractReader(ssc, spark, configConf) {
    var readerConf: KafkaReaderConf = _

    /**
      * 初始化
      *
      * @param cName
      * @param readerConfigStr
      */
    def init(cName: String, readerConfigStr: String) = {
        this.cName = cName

        readerConf = ConfUtil.getWholeConf(readerConfigStr, classOf[KafkaReaderConf])
        require(readerConf.getBrokerList != null, s"field ${Key.READER_BROKER_LIST} must be exists!")
        require(readerConf.getTopic != null, s"field ${Key.READER_TOPIC} must be exists!")
        require(readerConf.getGroupId != null, s"field ${Key.READER_GROUP_ID} must be exists!")
        require(readerConf.getParser != null, s"field ${Key.READER_PARSER} must be exists!")
    }

    override def primaryKey: String = {
        readerConf.topic + readerConf.groupId
    }

    override def tblTable: String = {
        Key.VIEW_PREFIX + readerConf.getTopic.replaceAll("-|\\.", "_")
    }

    /**
      * 读取数据
      */
    def read(): Unit = {
        val kafkaStream = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](Set(readerConf.topic), initKafkaParams())
        )
        //添加每次批次执行完的监听器
        ssc.addStreamingListener(new BatchStreamingListener(kafkaStream))

        val exchangeRunner = new ExchangeRunner(configConf,
            ssc, spark).init()
        kafkaStream.foreachRDD(rdd => {
            exchangeRunner.exchangeData(rdd, spark, new RowStream[ConsumerRecord[String, String]] {
                override def rowValue(row: ConsumerRecord[String, String]): String = {
                    row.value()
                }
            }, index)
        })
    }

    /**
      * 初始化kafka配置
      *
      * @return
      */
    private def initKafkaParams(): Map[String, Object] = {
        //默认kafka配置
        var kafkaParams = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> readerConf.getBrokerList,
            ConsumerConfig.GROUP_ID_CONFIG -> readerConf.getGroupId,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            //当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费 earliest/latest
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
            ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG -> "30000",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
        )
        //自定义kafka配置
        if (StringUtils.isNotBlank(readerConf.getExtraConfigs)) readerConf.getExtraConfigs.split(",").foreach(part => {
            val singleConfigParts = part.split(":")
            if (singleConfigParts.length == 2) kafkaParams += (singleConfigParts(0) -> singleConfigParts(1))
        })
        kafkaParams
    }
}
