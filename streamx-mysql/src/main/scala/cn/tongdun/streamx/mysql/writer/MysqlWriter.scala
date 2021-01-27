package cn.tongdun.streamx.mysql.writer

import java.lang
import java.sql.Connection

import cn.tongdun.streamx.core.api.writer.AbstractWriter
import cn.tongdun.streamx.core.entity.ConfigConf
import cn.tongdun.streamx.core.util.ConfUtil
import cn.tongdun.streamx.mysql.writer.util.DbUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-06-17
  *  \* Time: 18:38
  *  \* Description: 
  *  \*/
class MysqlWriter(spark: SparkSession, configConf: ConfigConf) extends AbstractWriter(spark, configConf) {
    val driver = "com.mysql.jdbc.Driver"
    var url: String = ""
    var user: String = ""
    var password: String = ""
    var table: String = ""
    var cols: ArrayBuffer[String] = _
    var sqlTemplate: String = ""
    var batchSize = 1000

    /**
      * 初始化
      *
      * @param cName
      * @param writerConfigStr
      */
    override def init(cName: String, writerConfigStr: String): Unit = {
        this.cName = cName

        val writerConf = ConfUtil.getWholeConf(writerConfigStr, classOf[MysqlConf])
        url = writerConf.getUrl
        user = writerConf.getUser
        password = writerConf.getPassword
        table = writerConf.getTable
        batchSize = writerConf.getBatchSize
        val conn = DbUtil.getConnection(url, user, password)
        cols = DbUtil.getFullColumns(table, conn)
        DbUtil.closeDbResources(conn = conn)
        sqlTemplate = upsertTemplate()
    }

    override def saveFails(): Boolean = {
        true
    }

    override def failMeta(): String = {
        table
    }

    /**
      * 写数据
      */
    override def write(df: DataFrame): Unit = {
        createView(df)
        val dfT = spark.sql(selectTmpSql())
        val schemaList = dfT.schema.toList

        dfT.foreachPartition(rows => {
            if (rows.nonEmpty) {
                var conn: Connection = null
                try {
                    conn = DbUtil.getConnection(url, user, password)
                    val preparedStatement = conn.prepareStatement(sqlTemplate)
                    var count = 0
                    rows.foreach(row => {
                        for (index <- schemaList.indices) {
                            preparedStatement.setObject(index + 1, getVal(row, index, schemaList.apply(index)))
                        }
                        preparedStatement.addBatch()
                        count += 1
                        if (count % batchSize == 0) {
                            preparedStatement.executeBatch()
                            conn.commit()
                            count = 0
                        }
                    })
                    preparedStatement.executeBatch()
                } finally {
                    DbUtil.closeDbResources(conn = conn, commit = true)
                }
            }
        })
    }

    private def getVal(row: Row, index: Int, schemaField: StructField) = {
        var res: Any = null
        if (row.get(index) != null && ("" + row.get(index)).length > 0) {
            res = getObjVal(row, index, schemaField)
        }
        res
    }

    private def getObjVal(r: Row, index: Int, sField: StructField) = {
        sField.dataType match {
            case IntegerType => new Integer(r.getInt(index))
            case DoubleType => new lang.Double(r.getDouble(index))
            case LongType => new java.lang.Long(r.getLong(index))
            case ShortType => new java.lang.Short(r.getShort(index))
            case FloatType => new java.lang.Float(r.getFloat(index))
            case BooleanType => new java.lang.Boolean(r.getBoolean(index))
            case _ => r.get(index).toString
        }
    }

    def upsertTemplate(): String = {
        def makeValues(nCols: Int): String = "(" + StringUtils.repeat("?", ",", nCols) + ")"

        def makeUpdatePart(cols: ArrayBuffer[String]): String = {
            val updatePart: ArrayBuffer[String] = ArrayBuffer.empty[String]
            for (col <- cols) {
                updatePart += (col + "=values(" + col + ")")
            }
            updatePart.mkString(",")
        }

        "INSERT INTO " + table + " (" + cols.mkString(",") + ") values " + makeValues(cols.length) +
                " ON DUPLICATE KEY UPDATE " + makeUpdatePart(cols)
    }
}
