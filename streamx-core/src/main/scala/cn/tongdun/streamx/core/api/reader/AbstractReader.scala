package cn.tongdun.streamx.core.api.reader

import cn.tongdun.streamx.core.entity.ConfigConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

/**
  * @Author hongyi.zhou
  * @Description
  * @Date create in 2020-04-06. 
  **/
abstract class AbstractReader(ssc: StreamingContext, spark: SparkSession, configConf: ConfigConf) extends Serializable {
    /**
      * 插件类名全路径
      */
    var cName: String = _
    /**
      * 当前reader在整体配置中的位置
      */
    var index: Int = _

    /**
      * 初始化
      *
      * @param cName
      * @param readerConfigStr
      */
    def init(cName: String, readerConfigStr: String): Unit

    /**
      * 读取数据
      */
    def read(): Unit

    /**
      * 表示reader唯一性主键
      *
      * @return
      */
    def primaryKey: String

    /**
      * 转临时表名
      *
      * @return
      */
    def tblTable: String

    def setIndex(index: Int): Unit = {
        this.index = index
    }
}
