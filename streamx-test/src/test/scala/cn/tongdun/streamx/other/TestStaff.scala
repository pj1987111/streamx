package cn.tongdun.streamx.other

import java.text.MessageFormat

import org.junit.Test

import scala.collection.mutable

/**
  * @Author hongyi.zhou
  * @Description
  * @Date create in 2020-04-17. 
  **/
class TestStaff {
    @Test
    def testPlaceHolder() = {
        val valList = "baidu1,baidu2,baidu3".split(",")
        println(MessageFormat.format("http://www.{0}.{1}.{2}.com", valList: _*))
    }

    @Test
    def testMapRemove(): Unit = {
        val stageDiagnosisList: mutable.LinkedHashMap[Int, String] = mutable.LinkedHashMap[Int, String]()
        for (index <- 0 until 100) {
            stageDiagnosisList += (index -> ("" + index))
        }
        println(stageDiagnosisList.size)
        removeMapOldData(stageDiagnosisList, 22)
        println(stageDiagnosisList.size)
    }

    def removeMapOldData[K](m: scala.collection.mutable.LinkedHashMap[K, _], keepNum: Int): Unit = {
        def removeOldestEntry(): m.type = m -= m.head._1

        if (m.size > keepNum * 1.5) {
            for (_ <- 0 until (m.size - keepNum)) {
                removeOldestEntry()
            }
        }
    }
}
