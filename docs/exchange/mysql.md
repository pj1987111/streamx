# 1 配置举例

```json
{
  "params": {
    "batchDuration": "30"
  },
  "plugins": [
  ],
  "configs": [
    {
      "reader": {
        "name": "com.zhy.streamx.kafka.reader.KafkaReader",
        "brokerList": "33.69.6.13:9092,33.69.6.14:9092,33.69.6.15:9092,33.69.6.16:9092,33.69.6.17:9092,33.69.6.18:9092",
        "topic": "baldur-gantry-original",
        "groupId": "dfs_gantry_rsu_baseinfo_20200707_1",
        "extraConfigs": "auto.offset.reset:latest",
        "parser": "canal"
      },
      "writers": [
        {
          "name": "com.zhy.streamx.mysql.writer.MysqlWriter",
          "url": "jdbc:mysql://ip:3306/gantry_status?useSSL=false",
          "user": "root",
          "password": "xxx",
          "table": "dfs_gantry_rsu_baseinfo"
        }
      ],
      "transformer": {
        "where": "type<>'DELETE' and table='dfs_gantry_rsu_baseinfo'",
        "sort": {
          "partitionKey": "database,table",
          "primaryKey": "GantryId,ControlId,StateVersion",
          "sortKey": "ts"
        },
        "reader_fields": {
          "database": {
            "type": "string"
          },
          "table": {
            "type": "string"
          },
          "type": {
            "type": "string"
          },
          "GantryId": {
            "type": "string"
          },
          "ControlId": {
            "type": "string"
          },
          "IPAddress": {
            "type": "string"
          },
          "Port": {
            "type": "int"
          },
          "RSUManuID": {
            "type": "string"
          },
          "RSUID": {
            "type": "string"
          },
          "StateVersion": {
            "type": "string"
          },
          "CreateTime": {
            "type": "string"
          },
          "RSUUpdateVersion": {
            "type": "string"
          },
          "HardWareVersion": {
            "type": "string"
          },
          "SoftWareVersion": {
            "type": "string"
          },
          "AntennaHardWareVersion": {
            "type": "string"
          },
          "AntennaSoftWareVersion": {
            "type": "string"
          },
          "AntennaNum": {
            "type": "int"
          },
          "PSAMNum": {
            "type": "int"
          },
          "RecvTime": {
            "type": "string"
          },
          "UploadFlag": {
            "type": "int"
          },
          "UploadTime": {
            "type": "string"
          },
          "LnduceFlag": {
            "type": "int"
          },
          "LnduceTime": {
            "type": "string"
          },
          "MUploadFlag": {
            "type": "int"
          },
          "MUploadTime": {
            "type": "string"
          },
          "ts": {
            "type": "bigint"
          }
        },
        "writer_fields": [
          "GantryId",
          "ControlId",
          "IPAddress",
          "Port",
          "RSUManuID",
          "RSUID",
          "StateVersion",
          "CreateTime",
          "RSUUpdateVersion",
          "HardWareVersion",
          "SoftWareVersion",
          "AntennaHardWareVersion",
          "AntennaSoftWareVersion",
          "AntennaNum",
          "PSAMNum",
          "RecvTime",
          "UploadFlag",
          "UploadTime",
          "LnduceFlag",
          "LnduceTime",
          "MUploadFlag",
          "MUploadTime",
          "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')"
        ]
      }
    }
  ]
}
```

# 2 配置详解

在这里主要介绍下数据源这块配置，其他几块稍微点一下。

## 2.1 writer部分

name hive的源这里配置的是`com.zhy.streamx.mysql.writer.MysqlWriter`这样写通过反射方式可以动态创建类。

url 这里填写jdbc的url链接，我们以mysql作为例子

user 这里填写数据库的用户名

password 这里填写数据库的密码

table 对应要写入的hive平台中的表名，使用库名.表名 这种方式

最主要的writer部分讲完了，再稍微介绍一下这里transformer部分

## 2.2 排序

这里先使用where来过滤部分数据，这里kafka输入数据是canal格式，其实就是一种json的格式。

这里着重讲解下transformer中sort用法，sort一般在有更新场景的情况下，防止乱序数据更新前后错乱的问题。

```json
{
 "sort": {
          "partitionKey": "database,table",
          "primaryKey": "GantryId,ControlId,StateVersion",
          "sortKey": "ts"
        }
}
```

rdd中获取的数据将根据partitionKey的设定groupby分区，在业务配置时，需要知道哪几个列可以作为分区，以达到并行处理的目的。

primaryKey和sortKey配合使用，设定的是数据的主键和排序的key。这里的业务还以是在分区后，每一批数据中同样的主键只取sortKey中最新的那条数据，这样就达到了乱序数据去重的需求。



mysql的写入是采用了insert for update方式，支持更新。