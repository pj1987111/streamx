package cn.tongdun.streamx.mysql.writer.util

import java.sql._

import org.apache.log4j.LogManager

import scala.collection.mutable.ArrayBuffer


/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-07-06
  *  \* Time: 15:16
  *  \* Description: 
  *  \*/
object DbUtil {
    @transient private lazy val logger = LogManager.getLogger(this.getClass)

    /**
      * 数据库连接的最大重试次数
      */
    private val MAX_RETRY_TIMES = 3

    @throws[SQLException]
    def getConnection(url: String, username: String, password: String): Connection = {
        var failed = true
        var dbConn: Connection = null
        var i = 0
        while (i < MAX_RETRY_TIMES && failed) {
            try {
                DriverManager.setLoginTimeout(10)
                if (username == null) {
                    dbConn = DriverManager.getConnection(url)
                } else {
                    dbConn = DriverManager.getConnection(url, username, password)
                }
                dbConn.createStatement.execute("select 111")
                failed = false
            } catch {
                case e: Exception =>
                    if (dbConn != null) dbConn.close()
                    if (i == MAX_RETRY_TIMES - 1) {
                        throw e
                    }
                    else {
                        try
                            Thread.sleep(3000)
                        catch {
                            case e: InterruptedException =>
                                throw new RuntimeException(e)
                        }
                    }
            }
            i = i + 1
        }
        dbConn.setAutoCommit(false)
        dbConn
    }

    def closeDbResources(rs: ResultSet = null, stmt: Statement = null, conn: Connection = null, commit: Boolean = false): Unit = {
        if (null != rs) {
            try {
                rs.close()
            } catch {
                case e: SQLException =>
                    logger.warn(s"Close resultSet error: ${e.getMessage}")
            }
        }
        if (null != stmt) {
            try {
                stmt.close()
            } catch {
                case e: SQLException =>
                    logger.warn(s"Close statement error: ${e.getMessage}")
            }
        }
        if (null != conn) {
            try {
                if (commit) {
                    commitConn(conn)
                }
                conn.close()
            } catch {
                case e: SQLException =>
                    logger.warn(s"Close connection error: ${e.getMessage}")
            }
        }
    }

    def commitConn(conn: Connection): Unit = {
        try {
            if (!conn.isClosed && !conn.getAutoCommit)
                conn.commit()
        } catch {
            case e: SQLException =>
                logger.warn(s"commit error: ${e.getMessage}")
        }
    }

    def executeBatch(dbConn: Connection, sqls: ArrayBuffer[String]): Unit = {
        if (sqls == null || sqls.isEmpty)
            return
        try {
            val stmt: Statement = dbConn.createStatement
            for (sql <- sqls) {
                stmt.addBatch(sql)
            }
            stmt.executeBatch
        } catch {
            case e: SQLException =>
                throw new RuntimeException("execute batch sql error:{}", e)
        } finally {
            commitConn(dbConn)
        }
    }

    @throws[SQLException]
    def getFullColumns(table: String, dbConn: Connection): ArrayBuffer[String] = {
        val ret = ArrayBuffer.empty[String]
        val rs = dbConn.getMetaData.getColumns(null, null, table, null)
        while (rs.next) {
            ret += rs.getString("COLUMN_NAME")
        }
        ret
    }
}
