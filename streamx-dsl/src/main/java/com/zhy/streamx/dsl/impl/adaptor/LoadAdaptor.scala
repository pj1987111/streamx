package com.zhy.streamx.dsl.impl.adaptor

import com.zhy.streamx.dsl.impl.ScriptSQLExecListener
import com.zhy.streamx.dsl.parser.DSLSQLParser
import com.zhy.streamx.dsl.parser.DSLSQLParser.{BooleanExpressionContext, ExpressionContext, FormatContext, PathContext, SqlContext, TableNameContext}

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2021-01-31
  *  \* Time: 21:41
  *  \* Description: 
  *  \*/
class LoadAdaptor(scriptSQLExecListener : ScriptSQLExecListener) extends DslAdaptor {

    def analyze(ctx: SqlContext): LoadStatement = {
        var format = ""
        var option = Map[String, String]()
        var path = ""
        var tableName = ""
        (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
            ctx.getChild(tokenIndex) match {
                case s: FormatContext =>
                    format = s.getText
                case s: ExpressionContext =>
                    option += (cleanStr(s.qualifiedName().getText) -> getStrOrBlockStr(s))
                case s: BooleanExpressionContext =>
                    option += (cleanStr(s.expression().qualifiedName().getText) -> getStrOrBlockStr(s.expression()))
                case s: PathContext =>
                    path = s.getText
                case s: TableNameContext =>
                    tableName = s.getText
                case _ =>
            }
        }
        LoadStatement(currentText(ctx), format, path, option, tableName)
    }

    override def parse(ctx: DSLSQLParser.SqlContext): Unit = {
        val LoadStatement(_, format, path, option, tableName) = analyze(ctx)
    }
}

case class LoadStatement(raw: String, format: String, path: String, option: Map[String, String] = Map[String, String](), tableName: String)

