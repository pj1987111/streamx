# 1 配置举例

```json
{
    "params":{
        "batchDuration":"300"
    },
    "configs":[
        {
            "reader":{
                "name":"",
                "brokerList":"33.69.6.13:9092,33.69.6.14:9092,33.69.6.15:9092,33.69.6.16:9092,33.69.6.17:9092,33.69.6.18:9092,33.69.6.19:9092,33.69.6.20:9092,33.69.6.21:9092,33.69.6.22:9092",
                "topic":"baldur-zj-road-condition",
                "groupId":"baldur-zj-road-condition-0922",
                "extraConfigs":"",
                "parser":"json"
            },
            "writers":[
                {
                    "name":"com.zhy.streamx.hive.writer.HiveWriter",
                    "tableName":"hhy_dw.baldur_zj_road_condition_dt",
                    "partitionValue":"ds",
                    "hiveSetConfigs":""
                }
            ],
            "transformer":{
                "where":"",
                "reader_fields":{
                    "adcode":{
                        "type":"string"
                    },
                    "data_version":{
                        "type":"string"
                    },
                    "ioi_id":{
                        "type":"string"
                    },
                    "ioi_type":{
                        "type":"string"
                    },
                    "road_level":{
                        "type":"int"
                    },
                    "speed":{
                        "type":"double"
                    },
                    "dt":{
                        "type":"string"
                    }
                },
                "writer_fields":[
                    "*"
                ]
            }
        },
        {
            "reader":{
                "name":"",
                "brokerList":"33.69.6.13:9092,33.69.6.14:9092,33.69.6.15:9092,33.69.6.16:9092,33.69.6.17:9092,33.69.6.18:9092,33.69.6.19:9092,33.69.6.20:9092,33.69.6.21:9092,33.69.6.22:9092",
                "topic":"dsp-province-police-bayonet",
                "groupId":"dsp-province-police-bayonet-0106-3",
                "extraConfigs":"",
                "parser":"json"
            },
            "writers":[
                {
                    "name":"com.zhy.streamx.hive.writer.HiveWriter",
                    "tableName":"hhy_dw.ods_province_police_traffic_avi_dt",
                    "partitionValue":"ds",
                    "hiveSetConfigs":""
                }
            ],
            "transformer":{
                "where":"sourceId='highroad'",
                "reader_fields":{
                    "sourceId":{
                        "type":"string"
                    },
                    "carWidth":{
                        "type":"string"
                    },
                    "plateTypeA":{
                        "type":"string"
                    },
                    "carDirect":{
                        "type":"string"
                    },
                    "deviceId":{
                        "type":"string"
                    },
                    "plateColorA":{
                        "type":"string"
                    },
                    "capTime":{
                        "type":"bigint"
                    }
                },
                "writer_fields":[
                    "*",                    
                    "if(lpad(captime,13,'1')>unix_timestamp()*1000+3600*1*10*1000,concat(from_unixtime(unix_timestamp(),'yyyyMMdd'),'-err'),from_unixtime(substr(lpad(captime,13,'1'),1,10),'yyyyMMdd'))"
                ]
            }
        }
    ]
}
```

# 2 配置详解

在这里主要介绍下数据源这块配置，其他几块稍微点一下。

## 2.1 writer部分



看一下writers这一块，streamx支持多writer写入。

name hive的源这里配置的是`com.zhy.streamx.hive.writer.HiveWriter`这样写通过反射方式可以动态创建类。

tableName 对应要写入的hive平台中的表名，使用库名.表名 这种方式。

partitionValue 表示分区列，非必选，如果有的话需要填上，系统会自动用动态分区插入的方式插入数据，而etl配置中的列需要在最后的列后面加上分区列的值。

多个分区列用逗号(,)分隔。

hiveSetConfigs 表示写入hive的一些额外配置，在spark官方文档上可以查到，这里举几个常用例子

set spark.merge.table.enabled = true 每一批跑完就merge
set spark.streaming.kafka.allowNonConsecutiveOffsets = true 如果遇到kafka的offset对不上，跳过以正常执行

## 2.2 其他配置

在这个configs配置中，可以看到配置了两套，这种配置方法可以使多个流任务跑在一个spark任务中，可以充分利用资源，简化管理。



在这里对第二块config处的transformer部分着重讲解一下。

where 这里是填写过滤条件，只有满足where条件里的数据才会写入目标源，如果不配where条件默认无过滤条件。

reader_fields中字段与kafka中字段一致。

writer_fields中，第一个字段*表示的含义是将reader_fields所有字段按照配置的顺序作为写入字段的列。

在这个例子里，配置了partitionValue=ds，表示存在分区列，所以这个时候需要在最后一个列添加一个列代表分区。这里是一个非常复杂的表达式，可以看到streamx支持复杂的etl，完全能hold住业务场景。

这里复杂etl函数的意思是将13位时间戳转化成字符串时间，如果超过10小时就写入yyyyMMdd-err分区中，正常的话写入yyyyMMdd分区中。

