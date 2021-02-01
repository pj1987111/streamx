package com.zhy.streamx.dsl

import com.zhy.streamx.dsl.impl.{ScriptSQLExec, ScriptSQLExecListener}
import org.junit.{Before, Test}

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2021-01-31
  *  \* Time: 22:19
  *  \* Description: 
  *  \*/
class ScriptTest1 {

    var listener: ScriptSQLExecListener = _

    @Before
    def init(): Unit = {
        listener = new ScriptSQLExecListener
    }

    @Test
    def test1(): Unit = {
        ScriptSQLExec.parse(
            """
              |load jdbc.`db.table` options
              |and driver="com.mysql.jdbc.Driver"
              |and url="jdbc:mysql://127.0.0.1:3306/...."
              |and user="..."
              |and password="...."
              |and prePtnArray = "age<=10 | age > 10"
              |and prePtnDelimiter = "\|"
              |as table1;
            """.stripMargin, listener)
    }
}
