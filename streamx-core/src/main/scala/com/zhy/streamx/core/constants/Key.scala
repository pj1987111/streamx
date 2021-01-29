package com.zhy.streamx.core.constants

/**
  * @Author hongyi.zhou
  * @Description 配置参数key
  * @Date create in 2019-12-23.
  **/
object Key {
    val NAME = "name"
    //全局配置块
    val PARAMS = "params"
    val PARAMS_BATCHDURATION = "batchDuration"

    //config 外层配置块
    val CONFIGS = "configs"
    //以下配置每个topic一个

    //reader 配置块 每个topic的特定配置
    val READER = "reader"
    //读取kafka的brokerlist地址
    val READER_BROKER_LIST = "brokerList"
    //读取kafka的topic名
    val READER_TOPIC = "topic"
    //读取kafka的consumer groupie
    val READER_GROUP_ID = "groupId"
    //专用kafka配置
    val READER_EXTRA_CONFIGS = "extraConfigs"
    val READER_PARSER = "parser"

    //转换块
    val TRANSFORMER = "transformer"
    val TRANSFORMER_READER_FIELDS = "reader_fields"
    val TRANSFORMER_WRITER_FIELDS = "writer_fields"
    val TRANSFORMER_WHERE = "where"

    //writer块
    val WRITERS = "writers"

    //properties 配置块，里面每个列表示kafka里的字段
    //key表示列名，里面的type表示类型。若是json嵌套列，列名用.隔开
    val COLUMN_TYPE = "type"
    val COLUMN_TYPE_TINYINT = "tinyint"
    val COLUMN_TYPE_SMALLINT = "smallint"
    val COLUMN_TYPE_INT = "int"
    val COLUMN_TYPE_BIGINT = "bigint"
    val COLUMN_TYPE_LONG = "long"
    val COLUMN_TYPE_BOOLEAN = "boolean"
    val COLUMN_TYPE_FLOAT = "float"
    val COLUMN_TYPE_DOUBLE = "double"
    val COLUMN_TYPE_STRING = "string"

    //临时视图前缀
    val VIEW_PREFIX = "tdl_tmp_"
    //parser解析器常量
    val JSON_PARSER = "json"
    val CSV_PARSER = "csv"
    val CANAL_PARSER = "canal"

    val PLUGIN_FAILOVER = "failover"
}
