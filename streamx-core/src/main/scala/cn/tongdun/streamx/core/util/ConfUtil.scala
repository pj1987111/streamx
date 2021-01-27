package cn.tongdun.streamx.core.util

import org.codehaus.jackson.map.{DeserializationConfig, ObjectMapper}

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-05-29
  *  \* Time: 10:38
  *  \* Description: 
  *  \*/
object ConfUtil {
    val mapper = new ObjectMapper()
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    def toStr(obj: Object):String = {
        mapper.writeValueAsString(obj)
    }

    def getRawConfObj(allConfStr: String): java.util.HashMap[String, Any] = {
        mapper.readValue(allConfStr, classOf[java.util.HashMap[String, Any]])
    }

    def getWholeConf[T](allConfStr: String, clz: Class[T]): T = {
        mapper.readValue(allConfStr, clz)
    }
}