package com.zhy.streamx.tools

import java.io.{BufferedReader, FileReader}

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.catalyst.util.StringUtils
import org.codehaus.jackson.map.{DeserializationConfig, ObjectMapper}
import org.json4s.jackson.Json
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, _}
import org.junit.Test

import scala.collection.JavaConversions._

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2021-01-06
  *  \* Time: 18:30
  *  \* Description: 
  *  \*/
class SchemaFileReader {
    implicit val formats = DefaultFormats

    val rootPath = "/Volumes/workspace/zhy/pass_workspace/streamx/streamx-test/src/test/resources/config_files/"

    @Test
    def test1() = {
        var count = 0
        val splitter = ","
        val fileName = "dsp-province-police-bayonet.csv"
        val file = rootPath + fileName
        val br = new BufferedReader(new FileReader(file))
        var createTable = s" create table $fileName ( \n"
        var line = ""
        var flag:Boolean= true

        try {
            while (flag) {
                if (line == null)
                    flag = false
                else {
                    line = br.readLine().trim()
                    count+=1

                    if (line.length() > 0) {
                        val firstI = line.indexOf(splitter, 0)
                        val secondI = line.indexOf(splitter, firstI+1)
                        createTable += line.substring(0, firstI)
                        createTable += " "
                        createTable += line.substring(firstI+1, secondI)
                        if(line.length>secondI) {
                            val comment = line.substring(secondI+1)
                            createTable += s" COMMENT '$comment'"
                        }
                        createTable += ",\n"
                    }
                }
            }
            createTable += ") COMMENT ' '\n"
            createTable +=
                    """
                      |partitioned by (
                      |	ds	string	comment	'分区'
                      |)
                      |lifecycle 365;
                    """.stripMargin
        } catch  {
            case e: Exception => {
            }
        }
        println(createTable)
    }

    @Test
    def test3() = {
        val splitter = ","
        val fileName = "dsp-province-police-bayonet.csv"
        val file = rootPath + fileName
        val br = new BufferedReader(new FileReader(file))
        var reader_fields = s"{"
        var line = ""
        var flag:Boolean= true

        try {
            while (flag) {
                if (line == null)
                    flag = false
                else {
                    line = br.readLine().trim()

                    if (line.length() > 0) {
                        val firstI = line.indexOf(splitter, 0)
                        val secondI = line.indexOf(splitter, firstI+1)
                        val name = line.substring(0, firstI)
                        val typo = line.substring(firstI+1, secondI)
                        reader_fields +=
                            s""""$name":{"type":"$typo"},""".stripMargin
                    }
                }
            }
            reader_fields += "}"
        } catch  {
            case e: Exception => {
            }
        }
        println(reader_fields)
    }

    @Test
    def test2(): Unit = {
        val msg =
            """
              |{"gantryId":"G001533006000310020","sectionName":"甬台温高速宁波二期","vehicleSpeed":71,"driveDir":1,"picTime":"2021-01-21 23:19:02","vehiclePlate":"鲁Q820DY_1","provinceType":"浙江省","intervalName":"宁海北互通至宁海互通","id":"G0015330060003100201102021012123000069","identifyType":"三型客车","vehicle":"鲁Q820DY_黄色"}
            """.stripMargin
        val values = parse(msg).extract[Map[String, Object]]
        val tabName = "dsp-province-police-bayonet"
        var createTable = s" create table $tabName ( \n"
        var readerFields = "{"
        for(valv <- values) {
            var line = ""
            val colName = valv._1
            val colVal = valv._2
            line+=colName
            var typo = "string"
            readerFields += s""""$colName":"""
            if(colVal.isInstanceOf[Double]) {
                typo = "double"
            } else if(colVal.isInstanceOf[Int] || colVal.isInstanceOf[Long] || colVal.isInstanceOf[BigInt]) {
                typo = "int"
            }
            line += s",$typo"
            readerFields += s"""{"type":"$typo"},"""
            println(line)
        }
        readerFields+="}"
        println()
        println(readerFields)
    }
}
