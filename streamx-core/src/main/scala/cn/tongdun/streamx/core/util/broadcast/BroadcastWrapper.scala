package cn.tongdun.streamx.core.util.broadcast

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext

import scala.reflect.ClassTag

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-06-01
  *  \* Time: 22:27
  *  \* Description: 广播变量支持更新
  *  \*/
case class BroadcastWrapper[T: ClassTag](@transient private val ssc: StreamingContext,
                                         @transient private val _v: T) {

    @transient private var v: Broadcast[T] = ssc.sparkContext.broadcast(_v)

    def update(newValue: T, blocking: Boolean = false): Unit = {
        v.unpersist(blocking)
        v = ssc.sparkContext.broadcast(newValue)
    }

    def value: T = v.value

    private def writeObject(out: ObjectOutputStream): Unit = {
        out.writeObject(v)
    }

    private def readObject(in: ObjectInputStream): Unit = {
        v = in.readObject().asInstanceOf[Broadcast[T]]
    }
}
