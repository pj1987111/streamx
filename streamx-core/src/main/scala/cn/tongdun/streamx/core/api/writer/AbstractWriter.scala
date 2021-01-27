package cn.tongdun.streamx.core.api.writer

import cn.tongdun.streamx.core.entity.ConfigConf
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author hongyi.zhou
  * @Description
  * @Date create in 2020-04-06. 
  **/
abstract class AbstractWriter(spark: SparkSession, configConf: ConfigConf) extends Serializable {
    /**
      * 插件类名全路径
      */
    var cName: String = _

    /**
      * 写数据
      */
    def write(df: DataFrame): Unit

    /**
      * 初始化
      *
      * @param cName
      * @param writerConfigStr
      */
    def init(cName: String, writerConfigStr: String): Unit

    /** 异常恢复需要实现以下两个函数 **/
    /**
      * 是否使用异常恢复机制
      *
      * @return
      */
    def saveFails(): Boolean = {
        false
    }

    /**
      * 异常恢复机制，路径的key，异常文件存放的最子目录
      *
      * @return
      */
    def failMeta(): String = {
        ""
    }

    def createView(df: DataFrame) = {
        df.createOrReplaceTempView(configConf.tblTable)
    }

    /**
      * 通用etl函数清理sql
      *
      * @return
      */
    def selectTmpSql(): String = {
        val transformerFields = configConf.getTransformer.transformerFields
        val tdlTableName = configConf.tblTable
        val transformerWhere = configConf.getTransformer.getWhere
        val whereValue = if (StringUtils.isBlank(transformerWhere)) "" else s" where $transformerWhere "
        val selectTmpSql = s"select $transformerFields from $tdlTableName $whereValue"
        selectTmpSql
    }
}
