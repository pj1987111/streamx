package cn.tongdun.streamx.addon.failover

import java.io.File
import java.util.concurrent.ThreadPoolExecutor

import cn.tongdun.streamx.core.ExchangeRunner
import cn.tongdun.streamx.core.api.plugin.AbstractFailoverPlugin
import cn.tongdun.streamx.core.api.writer.AbstractWriter
import cn.tongdun.streamx.core.constants.Key
import cn.tongdun.streamx.core.entity.StreamxConf
import cn.tongdun.streamx.core.util.{ConfUtil, StreamUtils, ThreadUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.log4j.LogManager
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable.ArrayBuffer

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-05-28
  *  \* Time: 10:13
  *  \* Description: 基于hdfs的容错插件
  *  \*/
class FailoverPlugin(ssc: StreamingContext, spark: SparkSession, streamxConf: StreamxConf)
        extends AbstractFailoverPlugin(ssc, spark, streamxConf) {
    @transient private lazy val logger = LogManager.getLogger(classOf[FailoverPlugin])

    var failRoot: String = ""
    var sleepSecond: Int = 3 * 60
    val SECOND: Int = 1000
    var threadPool: ThreadPoolExecutor = _
    var listenerMap: ArrayBuffer[((Int, String), AbstractWriter)] = ArrayBuffer.empty[((Int, String), AbstractWriter)]

    /**
      * 初始化
      *
      * @param cName
      * @param pluginConfigStr
      */
    override def init(cName: String, pluginConfigStr: String): Unit = {
        this.cName = cName

        val pluginConf = ConfUtil.getWholeConf(pluginConfigStr, classOf[FailoverPluginConf])
        failRoot = pluginConf.getFailDir
        sleepSecond = pluginConf.getSleepSec
        if (StringUtils.isBlank(failRoot)) {
            logger.warn("failRoot is not set,this plugin is not start")
            return
        }
        initFailDir(failRoot)
        for (index <- streamxConf.configs.indices) {
            val eachConfig = streamxConf.configs.apply(index)
            for (writer <- eachConfig.getWriters) {
                val ab = StreamUtils.initWriter(
                    writer.get(Key.NAME).asInstanceOf[String],
                    spark, eachConfig, ConfUtil.toStr(writer))
                if (ab.saveFails() && StringUtils.isNotBlank(ab.failMeta())) {
                    listenerMap += (index, ab.failMeta()) -> ab
                }
            }
        }
        threadPool = ThreadUtils.newDaemonCachedThreadPool("FailoverPlugin", listenerMap.size * 3, 60)
    }

    /**
      * 执行
      */
    override def run(): Unit = {
        if (StringUtils.isBlank(failRoot))
            return
        for (listener <- listenerMap) {
            threadPool.submit(new Runnable {
                override def run(): Unit = {
                    while (true) {
                        val index = listener._1._1
                        val tableName = listener._1._2
                        val listenerPath = new File(failRoot, tableName).getPath
                        val ab = listener._2
                        logger.info(s"begin to handle fail files listenerPath:$listenerPath")

                        val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
                        val files = hdfs.listStatus(new Path(listenerPath), new PathFilter {
                            override def accept(path: Path): Boolean = {
                                path.toString.endsWith(".fail")
                            }
                        })
                        for (fileStatus <- files) {
                            val errPath: Path = fileStatus.getPath
                            try {
                                val errDf = spark.read.parquet(errPath.toString)
                                val hasErr = ExchangeRunner.execute(spark, errDf, ab, index)
                                if (!hasErr) {
                                    hdfs.delete(errPath, true)
                                    logger.info(s"success in handling fail file listenerPath:${errPath.toString}")
                                }
                            } catch {
                                //防止解析错误一直重复
                                case ae: AnalysisException => {
                                    hdfs.rename(errPath, new Path(errPath.toString + "err"))
                                }
                                case e: Exception => {
                                    logger.error(e.getMessage, e)
                                }
                            }
                        }
                        hdfs.close()
                        Thread.sleep(sleepSecond * SECOND)
                    }
                }
            })
        }
    }

    /**
      * 初始化文件系统对应的目录
      *
      * @param failDir
      */
    def initFailDir(failDir: String): Unit = {
        val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val failDirPath = new Path(failDir)
        if (!hdfs.exists(failDirPath))
            hdfs.mkdirs(failDirPath)
        hdfs.close()
        logger.info(s"init failDirPath done,path is $failDirPath")
        logger.info(s"init failDirPath done,path is $failDirPath")
    }

    /**
      * 目录规则
      * 配置设定目录/failMeta(hive为表名)/fail_$time.fail
      *
      * @param df
      * @param failMeta
      */
    override def saveFailFiles(df: DataFrame, failMeta: String): Unit = {
        if (StringUtils.isBlank(failRoot))
            return
        val time = System.currentTimeMillis()
        val path = s"$failRoot/$failMeta/fail_$time.fail"
        //记录数据信息
        df.write.parquet(path)
    }
}
