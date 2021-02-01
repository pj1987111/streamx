package com.zhy.streamx.dsl.impl.adaptor

import com.zhy.streamx.dsl.parser.DSLSQLLexer
import com.zhy.streamx.dsl.parser.DSLSQLParser.{ExpressionContext, SqlContext}
import org.antlr.v4.runtime.misc.Interval

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2021-01-31
  *  \* Time: 21:31
  *  \* Description: 
  *  \*/
trait DslAdaptor extends DslTool {
    def parse(ctx: SqlContext): Unit
}

trait DslTool {

//    def branchContext = {
//        ScriptSQLExec.context().execListener.branchContext.contexts
//    }

    def currentText(ctx: SqlContext) = {
        val input = ctx.start.getTokenSource().asInstanceOf[DSLSQLLexer]._input

        val start = ctx.start.getStartIndex()
        val stop = ctx.stop.getStopIndex()
        val interval = new Interval(start, stop)
        input.getText(interval)
    }

    def cleanStr(str: String) = {
        if (str.startsWith("`") || str.startsWith("\"") || (str.startsWith("'") && !str.startsWith("'''")))
            str.substring(1, str.length - 1)
        else str
    }


    def cleanBlockStr(str: String) = {
        if (str.startsWith("'''") && str.endsWith("'''"))
            str.substring(3, str.length - 3)
        else str
    }

    def getStrOrBlockStr(ec: ExpressionContext) = {
        if (ec.STRING() == null || ec.STRING().getText.isEmpty) {
            cleanBlockStr(ec.BLOCK_STRING().getText)
        } else {
            cleanStr(ec.STRING().getText)
        }
    }

    def withPathPrefix(prefix: String, path: String): String = {

        val newPath = cleanStr(path)
        if (prefix.isEmpty) return newPath

        if (path.contains("..")) {
            throw new RuntimeException("path should not contains ..")
        }
        if (path.startsWith("/")) {
            return prefix + path.substring(1, path.length)
        }
        return prefix + newPath

    }

//    def withPathPrefix(context: MLSQLExecuteContext, path: String): String = {
//        withPathPrefix(context.home, path)
//    }

    def parseDBAndTableFromStr(str: String) = {
        val cleanedStr = cleanStr(str)
        val dbAndTable = cleanedStr.split("\\.")
        if (dbAndTable.length > 1) {
            val db = dbAndTable(0)
            val table = dbAndTable.splitAt(1)._2.mkString(".")
            (db, table)
        } else {
            (cleanedStr, cleanedStr)
        }

    }

//    def resourceRealPath(scriptSQLExecListener: ScriptSQLExecListener,
//                         resourceOwner: Option[String],
//                         path: String): String = {
//        withPathPrefix(scriptSQLExecListener.pathPrefix(resourceOwner), cleanStr(path))
//    }

//    def parseRef(format: String, path: String, separator: String, callback: Map[String, String] => Unit) = {
//        var finalPath = path
//        var dbName = ""
//
//        val firstIndex = finalPath.indexOf(separator)
//
//        if (firstIndex > 0) {
//
//            dbName = finalPath.substring(0, firstIndex)
//            finalPath = finalPath.substring(firstIndex + 1)
//            ConnectMeta.presentThenCall(DBMappingKey(format, dbName), options => callback(options))
//        }
//
//        Array(dbName, finalPath)
//    }
}
