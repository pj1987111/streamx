# 1 配置举例

先从一个例子来介绍streamx的配置。

```json
{
  "params":{
    "batchDuration":"60"
  },
  "configs":[
    {
      "reader":{
        "name":"",
        "brokerList":"33.69.6.13:9092,33.69.6.14:9092,33.69.6.15:9092,33.69.6.16:9092,33.69.6.17:9092,33.69.6.18:9092,33.69.6.19:9092,33.69.6.20:9092,33.69.6.21:9092,33.69.6.22:9092",
        "topic":"baldur-etc-portal",
        "groupId":"baldur-etc-portal_1",
        "extraConfigs":"auto.offset.reset:latest",
        "parser":"json"
      },
      "writers":[
        {
          "name":"com.zhy.streamx.hive.writer.HiveWriter",
          "tableName":"hhy_dw.ods_chg_rt_dfs_gantry_travelimage_dt",
          "partitionValue":"ds",
          "hiveSetConfigs":""
        }
      ],
      "transformer":{
        "where":"",
        "reader_fields":{
          "PicId": {
            "type": "string"
          },
          "GantryId": {
            "type": "string"
          },
          "GantryOrderNum": {
            "type": "int"
          },
          "GantryHex": {
            "type": "string"
          },
          "PicTime": {
            "type": "string"
          },
          "DriveDir": {
            "type": "int"
          },
          "CameraNum": {
            "type": "int"
          },
          "HourBatchNo": {
            "type": "string"
          },
          "ShootPosition": {
            "type": "int"
          },
          "LaneNum": {
            "type": "string"
          },
          "VehiclePlate": {
            "type": "string"
          },
          "VehicleSpeed": {
            "type": "int"
          },
          "IdentifyType": {
            "type": "int"
          },
          "VehicleModel": {
            "type": "string"
          },
          "VehicleColor": {
            "type": "string"
          },
          "ImageSize": {
            "type": "int"
          },
          "LicenseImageSize": {
            "type": "int"
          },
          "BinImageSize": {
            "type": "int"
          },
          "Reliability": {
            "type": "string"
          },
          "VehFeatureCode": {
            "type": "string"
          },
          "FaceFeatureCode": {
            "type": "string"
          },
          "VerifyCode": {
            "type": "string"
          },
          "TradeId": {
            "type": "string"
          },
          "MatchStatus": {
            "type": "int"
          },
          "ValidStatus": {
            "type": "int"
          },
          "DealStatus": {
            "type": "int"
          },
          "RelatedPicId": {
            "type": "string"
          },
          "AllRelatedPicId": {
            "type": "string"
          },
          "StationDBTime": {
            "type": "string"
          },
          "StationDealTime": {
            "type": "string"
          },
          "StationValidTime": {
            "type": "string"
          },
          "StationMatchTime": {
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
          "CognateSerial": {
            "type": "string"
          },
          "AllCognateSerial": {
            "type": "string"
          },
          "MUploadFlag": {
            "type": "int"
          },
          "MUploadTime": {
            "type": "string"
          },
          "ChargeUnitId": {
            "type": "string"
          },
          "TransDate": {
            "type": "string"
          },
          "VersionInfo": {
            "type": "string"
          },
          "Spare1": {
            "type": "string"
          },
          "Spare2": {
            "type": "string"
          },
          "Spare3": {
            "type": "string"
          },
          "Spare4": {
            "type": "string"
          },
          "Spare5": {
            "type": "string"
          },
          "Spare6": {
            "type": "string"
          },
          "Spare7": {
            "type": "string"
          },
          "Spare8": {
            "type": "string"
          }
        },
        "writer_fields":[
          "PicId",
          "GantryId",
          "GantryOrderNum",
          "GantryHex",
          "PicTime",
          "DriveDir",
          "CameraNum",
          "HourBatchNo",
          "ShootPosition",
          "LaneNum",
          "VehiclePlate",
          "VehicleSpeed",
          "IdentifyType",
          "VehicleModel",
          "VehicleColor",
          "ImageSize",
          "LicenseImageSize",
          "BinImageSize",
          "Reliability",
          "VehFeatureCode",
          "FaceFeatureCode",
          "VerifyCode",
          "TradeId",
          "MatchStatus",
          "ValidStatus",
          "DealStatus",
          "RelatedPicId",
          "AllRelatedPicId",
          "StationDBTime",
          "StationDealTime",
          "StationValidTime",
          "StationMatchTime",
          "UploadFlag",
          "UploadTime",
          "LnduceFlag",
          "LnduceTime",
          "CognateSerial",
          "AllCognateSerial",
          "MUploadFlag",
          "MUploadTime",
          "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')",
          "ChargeUnitId",
          "TransDate",
          "VersionInfo",
          "Spare1",
          "Spare2",
          "Spare3",
          "Spare4",
          "Spare5",
          "Spare6",
          "Spare7",
          "Spare8",
          "concat(substr(HourBatchNo,1,4),substr(HourBatchNo,5,2),substr(HourBatchNo,7,2))"
        ]
      }
    }
  ]
}
```

# 2 配置详解

## 2.1 params 部分

batchDuration 调度间隔，单位秒，配置中所有任务都会按照这个间隔调度

## 2.2 configs 部分

  configs 节点保存一个数组，对应多个config，每个config都包括reader，writer和transformer三个部分，分别对应流交换中的数据源端，数据目标端以及etl转化的配置。

### 2.2.1 reader部分

reader 节点表示数据接入源，目前reader仅支持kafka这个类型

name 数据源端的类名，目前默认支持kafka，可以写com.zhy.streamx.kafka.reader.KafkaReader或者空字符串

brokerList 对接kafka的brokerlist，多个地址之间用逗号分隔，端口默认是9092

topic 对接kafka的topic

groupId 对接kafka的topic的groupId

extraConfigs 对接kafka的额外参数，参数之间用逗号分隔，key和value之间用冒号分隔

parser 解析数据的解析器，目前支持json，csv和canal三种格式

### 2.2.2 writer部分

节点表示数据写入源，支持多个，用数组表示，目前支持写入elasticsearch，redis，hive，jdbc。

每个数据源的配置都会有一定的区别，具体每个数据源的配置将分多个配置案例来展示。

### 2.2.3 transformer部分

transformer 表示实时交换过程中的etl操作。

reader_fields 表示源端读取字段，字段名需要与kafka中的字段一致(包括大小写)，type表示解析的类型。目前包括int/long/boolean/float/double/string 这几种，这个配置中，默认会给每个列按照配置的顺序定义列顺序，比如这里的配置的列从上到下分别是1....n列号。

writer_fields 表示目标端写入字段，字段名需要和reader_fields中定义的保持一致，如果是写入hive的话这里字段的顺序需要和hive列严格对齐。可以用*来代表所有定义在reader_fields中的字段，并按照其顺序。

同时支持函数，可以使用spark和hive函数来处理列。比如在例子中

from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')来获得当前字符串时间

concat(substr(HourBatchNo,1,4),substr(HourBatchNo,5,2),substr(HourBatchNo,7,2)) 对字符串时间做拆分获得年月日，通常用来写入hive分区中。





