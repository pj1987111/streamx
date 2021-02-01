package com.zhy.streamx.socket.reader

import com.zhy.streamx.core.ExchangeRunner
import com.zhy.streamx.core.api.reader.{AbstractReader, RowStream}
import com.zhy.streamx.core.constants.Key
import com.zhy.streamx.core.entity.ConfigConf
import com.zhy.streamx.core.util.ConfUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2021-01-29
  *  \* Time: 16:37
  *  \* Description: 
  *  \*/
class SocketReader(ssc: StreamingContext, spark: SparkSession, configConf: ConfigConf)
        extends AbstractReader(ssc, spark, configConf) {
    var readerConf: SocketReaderConf = _
    var ip: String = "127.0.0.1"
    var port: Int = 7777

    /**
      * 初始化
      *
      * @param cName
      * @param readerConfigStr
      */
    override def init(cName: String, readerConfigStr: String): Unit = {
        this.cName = cName

        readerConf = ConfUtil.getWholeConf(readerConfigStr, classOf[SocketReaderConf])
        ip = readerConf.getIp
        port = readerConf.getPort.toInt
    }

    /**
      * 读取数据
      */
    override def read(): Unit = {
        val dStream: ReceiverInputDStream[String] = ssc.socketTextStream(ip, port)

        val exchangeRunner = new ExchangeRunner(configConf, spark).init()
        dStream.foreachRDD(rdd => {
            exchangeRunner.exchangeData(rdd, new RowStream[String] {
                override def rowValue(row: String): String = {
                    row
                }
            }, index)
        })
    }

    /**
      * 表示reader唯一性主键
      *
      * @return
      */
    override def primaryKey: String = {
        readerConf.ip + readerConf.port
    }

    /**
      * 转临时表名
      *
      * @return
      */
    override def tblTable: String = {
        Key.VIEW_PREFIX + "_nc_" + System.currentTimeMillis
    }
}
