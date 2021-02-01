package com.zhy.streamx.dsl.impl.adaptor

import com.zhy.streamx.dsl.impl.ScriptSQLExecListener
import com.zhy.streamx.dsl.parser.DSLSQLParser

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2021-01-31
  *  \* Time: 21:41
  *  \* Description: 
  *  \*/
class SaveAdaptor (scriptSQLExecListener : ScriptSQLExecListener) extends DslAdaptor {
    override def parse(ctx: DSLSQLParser.SqlContext): Unit = {

    }
}
