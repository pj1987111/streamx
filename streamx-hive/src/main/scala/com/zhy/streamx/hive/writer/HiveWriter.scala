package com.zhy.streamx.hive.writer

import com.zhy.streamx.core.api.writer.AbstractWriter
import com.zhy.streamx.core.entity.ConfigConf
import com.zhy.streamx.core.util.ConfUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

/**
  * @Author hongyi.zhou
  * @Description
  * @Date create in 2020-04-06.
  **/
class HiveWriter(spark: SparkSession, configConf: ConfigConf) extends AbstractWriter(spark, configConf) {
    /**
      * hive写入表名
      */
    var tableName = ""
    /**
      * 分区列，多个用逗号分隔，若为空，表示不含分区字段，hive语句将不包含PARTITION，以及不设置
      * set hive.exec.dynamic.partition = true 和 set hive.exec.dynamic.partition.mode = nonstrict
      */
    var partitionValue = ""
    /**
      * hive额外set值
      * 例:
      * 是否跑完merge : set spark.merge.table.enabled = true
      * 如果遇到offset对不上，是否跳过 : set spark.streaming.kafka.allowNonConsecutiveOffsets = true
      */
    var hiveSetConfigs = ""

    //todo cross hive
    override def init(cName: String, writerConfigStr: String): Unit = {
        this.cName = cName

        val writerConf = ConfUtil.getWholeConf(writerConfigStr, classOf[HiveConf])
        tableName = writerConf.getTableName
        partitionValue = writerConf.getPartitionValue
        hiveSetConfigs = writerConf.getHiveSetConfigs
        require(tableName != null, s"field tableName must be exists!")
        if (writerConf.getClusterConfig != null && writerConf.getClusterConfig.size() > 0) {
            val sparkConf = spark.sparkContext.getConf
            writerConf.getClusterConfig.asScala.foreach(conf => {
                sparkConf.set(conf._1, conf._2)
            })
        }
    }

    override def saveFails(): Boolean = {
        true
    }

    override def failMeta(): String = {
        tableName
    }

    override def write(df: DataFrame): Unit = {
        createView(df)
        hiveParamsInitialized()
        spark.sql(makeInsertSql())
    }

    /**
      * hive动态设置参数初始化
      */
    def hiveParamsInitialized(): Unit = {
        if (StringUtils.isNotBlank(partitionValue)) {
            val setDynamicP = s"set hive.exec.dynamic.partition = true"
            val setDynamicM = s"set hive.exec.dynamic.partition.mode = nonstrict"
            spark.sql(setDynamicP)
            spark.sql(setDynamicM)
        }
        if (StringUtils.isNotBlank(hiveSetConfigs)) {
            hiveSetConfigs.split(",").foreach(hiveSetConfig => {
                spark.sql(hiveSetConfig)
            })
        }
    }

    /**
      * 设置插入语句
      *
      * @return
      */
    def makeInsertSql() = {
        val p = if (StringUtils.isBlank(partitionValue)) "" else s" PARTITION($partitionValue) "
        val insertSql = s"insert into table $tableName $p ${selectTmpSql()}"
        insertSql
    }
}
