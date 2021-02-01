package com.zhy.streamx.dsl.impl

import com.zhy.streamx.dsl.impl.adaptor.{DslAdaptor, LoadAdaptor, SaveAdaptor}
import com.zhy.streamx.dsl.parser.{DSLSQLBaseListener, DSLSQLLexer, DSLSQLListener, DSLSQLParser}
import com.zhy.streamx.dsl.parser.DSLSQLParser.SqlContext
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.ParseTreeWalker

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2021-01-31
  *  \* Time: 21:23
  *  \* Description: 
  *  \*/
object ScriptSQLExec {
    def parse(input: String, listener: DSLSQLListener): Unit = {
        _parse(input, listener)
    }
    def _parse(input: String, listener: DSLSQLListener) = {
        val loadLexer = new DSLSQLLexer(new CaseChangingCharStream(input))
        val tokens = new CommonTokenStream(loadLexer)
        val parser = new DSLSQLParser(tokens)

//        parser.setErrorHandler(new MLSQLErrorStrategy)
//        parser.addErrorListener(new MLSQLSyntaxErrorListener())

        val stat = parser.statement()
        ParseTreeWalker.DEFAULT.walk(listener, stat)
    }
}

case class BranchContextHolder(contexts: mutable.Stack[BranchContext],traces:ArrayBuffer[String])

trait BranchContext

//case class IfContext(sqls: mutable.ArrayBuffer[DslAdaptor],
//                     ctxs: mutable.ArrayBuffer[SqlContext],
//                     variableTable: VariableTable,
//                     shouldExecute: Boolean,
//                     haveMatched: Boolean,
//                     skipAll: Boolean) extends BranchContext

case class ForContext() extends BranchContext

class ScriptSQLExecListener extends DSLSQLBaseListener {

    private val _branchContext = BranchContextHolder(new mutable.Stack[BranchContext](),new ArrayBuffer[String]())


    def branchContext = {
        _branchContext
    }

    override def exitSql(ctx: SqlContext): Unit = {
        def getText = {
            val input = ctx.start.getTokenSource().asInstanceOf[DSLSQLLexer]._input

            val start = ctx.start.getStartIndex()
            val stop = ctx.stop.getStopIndex()
            val interval = new Interval(start, stop)
            input.getText(interval)
        }

//        def before(clzz: String) = {
//            _jobListeners.foreach(_.before(clzz, getText))
//        }
//
//        def after(clzz: String) = {
//            _jobListeners.foreach(_.after(clzz, getText))
//        }
//
//        def traceBC = {
//            ScriptSQLExec.context().execListener.env().getOrElse("__debug__","false").toBoolean
//        }

        def str(ctx:SqlContext)  = {

            val input = ctx.start.getTokenSource().asInstanceOf[DSLSQLLexer]._input

            val start = ctx.start.getStartIndex()
            val stop = ctx.stop.getStopIndex()
            val interval = new Interval(start, stop)
            input.getText(interval)
        }

        def execute(adaptor: DslAdaptor) = {
            val bc = branchContext.contexts
            if (!bc.isEmpty) {
//                bc.pop() match {
//                    case ifC: IfContext =>
//                        val isBranchCommand = adaptor match {
//                            case a: TrainAdaptor =>
//                                val TrainStatement(_, _, format, _, _, _) = a.analyze(ctx)
//                                val isBranchCommand = (format == "IfCommand"
//                                        || format == "ElseCommand"
//                                        || format == "ElifCommand"
//                                        || format == "FiCommand"
//                                        || format == "ThenCommand")
//                                isBranchCommand
//                            case _ => false
//                        }
//
//                        if (ifC.skipAll) {
//                            bc.push(ifC)
//                            if(isBranchCommand){
//                                adaptor.parse(ctx)
//                            }
//                        } else {
//                            if (ifC.shouldExecute && !isBranchCommand) {
//                                ifC.sqls += adaptor
//                                ifC.ctxs += ctx
//                                bc.push(ifC)
//                            } else if (!ifC.shouldExecute && !isBranchCommand) {
//                                bc.push(ifC)
//                                // skip
//                            }
//                            else {
//                                bc.push(ifC)
//                                adaptor.parse(ctx)
//                            }
//                        }
//                    case forC: ForContext =>
//                }
            } else {
//                if(traceBC) {
//                    logInfo(format(s"SQL:: ${str(ctx)}"))
//                }
                adaptor.parse(ctx)
            }
        }

        val PREFIX = ctx.getChild(0).getText.toLowerCase()

//        before(PREFIX)
        PREFIX match {
            case "load" =>
                val adaptor = new LoadAdaptor(this)
                execute(adaptor)

//            case "select" =>
//                val adaptor = new SelectAdaptor(this)
//                execute(adaptor)

            case "save" =>
                val adaptor = new SaveAdaptor(this)
                execute(adaptor)
//            case "connect" =>
//                val adaptor = new ConnectAdaptor(this)
//                execute(adaptor)
//            case "create" =>
//                val adaptor = new CreateAdaptor(this)
//                execute(adaptor)
//            case "insert" =>
//                val adaptor = new InsertAdaptor(this)
//                execute(adaptor)
//            case "drop" =>
//                val adaptor = new DropAdaptor(this)
//                execute(adaptor)
//            case "refresh" =>
//                val adaptor = new RefreshAdaptor(this)
//                execute(adaptor)
//            case "set" =>
//                val adaptor = new SetAdaptor(this, Stage.physical)
//                execute(adaptor)
//            case "train" | "run" | "predict" =>
//                val adaptor = new TrainAdaptor(this)
//                execute(adaptor)
//            case "register" =>
//                val adaptor = new RegisterAdaptor(this)
//                execute(adaptor)
            case _ => throw new RuntimeException(s"Unknow statement:${ctx.getText}")
        }
//        after(PREFIX)
    }
}
