package com.zhy.streamx.parser

import cn.tongdun.streamx.core.config.ConfigParser.reload
import cn.tongdun.streamx.core.parser.{CanalParser, CsvParser, JsonParser}
import cn.tongdun.streamx.core.util.StreamUtils
import org.apache.spark.sql.Row
import org.codehaus.jackson.map.{DeserializationConfig, ObjectMapper}
import org.codehaus.jackson.node.TextNode
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-05-30
  *  \* Time: 21:45
  *  \* Description: 
  *  \*/
class ParserTest {
    @Test
    def csvTest(): Unit = {
        val config = StreamUtils.readConfig("local:/Volumes/workspace/zhy/pass_workspace/streamx/streamx-test/src/test/resources/data/${project.version}/parser_test1.json")
        val streamxConf = reload(config)
        val parser = new CsvParser()
        val row = "id1,123.456,234.567,20200530,12:50,123456"
        val rowsAb = ArrayBuffer.empty[Row]
        parser.parseRow(row, streamxConf.configs.head.transformer.readerSchema, rowsAb)
        println(rowsAb)
    }

    @Test
    def jsonTest(): Unit = {
        val row =
            """
              |{
              |  "type": "SENDING",
              |  "backTestTaskId": "384ccb0aa2ea456b902baf403dd3cf45",
              |  "msg": {
              |    "activity_e_dt_vs_invoketype": "20",
              |    "activity_d_t_vb_eventoccurtime": 1581994755000,
              |    "eventresult": "{\"eventId\":\"test02_abc\",\"policySetName\":\"CYB测试2\",\"partnerCode\":\"kratos\",\"appName\":\"test02\",\"ruleList\":[\"自定义规则\"],\"policyList\":[\"测试2\"],\"eventType\":\"Login\",\"sequenceId\":\"1581994755324-51446295\",\"finalDealType\":\"Reject\",\"riskType\":[\"bruteForce\"],\"finalDealTypeName\":\"拒绝\",\"riskStatus\":\"Reject\",\"riskScore\":111,\"ruleUuids\":[\"317d1386b9ca49f9a010ede9b1bebcd2\"],\"timestamp\":1581995076972}",
              |    "activity_s_dt_vb_blackbox": "888",
              |    "activity_s_dt_vs_appname": "test02",
              |    "activity_eventtype": "Login",
              |    "browser": {"sequenceId":"1581994755324-51446295"},
              |    "a": {"b":{"c":"d"}},
              |    "device": {"sequenceId":"1581994755324-51446295"},
              |    "activity_s_dt_vs_partnercode": "kratos",
              |    "activity_eventoccurtime": 1581994755000,
              |    "activity_s_dt_vs_eventid": "test02_abc",
              |    "activity_sequenceid": "1581994755324-51446295",
              |    "activity_e_c_vb_cardtype": "56",
              |    "policy": "{\"hitRules\":[{\"score\":111,\"policyUuid\":\"07557b6f0ec5419fa7284391e974c90d\",\"policyVersion\":104,\"name\":\"自定义规则\",\"ruleDetailDataList\":[{\"systemField\":\"E_C_VB_CARDTYPE\",\"data\":\"56\",\"description\":\"卡片类型\",\"type\":\"systemField\"}],\"uuid\":\"317d1386b9ca49f9a010ede9b1bebcd2\"}],\"policySet\":[{\"policyUuid\":\"07557b6f0ec5419fa7284391e974c90d\",\"policyVersion\":104,\"policyScore\":111,\"riskType\":\"bruteForce\",\"policyName\":\"测试2\",\"dealType\":\"Reject\",\"policyMode\":\"Weighted\"}]}",
              |    "salaxyzbs": "{}",
              |    "geo": "{\"sequenceId\":\"1581994755324-51446295\"}",
              |    "offlinezbs": "{}"
              |  }
              |}
            """.stripMargin

        val config = StreamUtils.readConfig("local:/Volumes/workspace/zhy/pass_workspace/streamx/streamx-test/src/test/resources/data/${project.version}/jackson_test1.json")
        val streamxConf = reload(config)
        val rowsAb = ArrayBuffer.empty[Row]
        val parser = new JsonParser
        parser.parseRow(row, streamxConf.getConfigs.head.transformer.readerSchema, rowsAb)
        println(rowsAb)
    }

    @Test
    def jsonTest2(): Unit = {
        val row =
            """
              |[]
            """.stripMargin

        val config = StreamUtils.readConfig("local:/Volumes/workspace/zhy/pass_workspace/streamx/streamx-test/src/test/resources/data/${project.version}/jackson_test1.json")
        val streamxConf = reload(config)
        val rowsAb = ArrayBuffer.empty[Row]
        val parser = new JsonParser
        parser.parseRow(row, streamxConf.getConfigs.head.transformer.readerSchema, rowsAb)
        println(rowsAb)
    }

    @Test
    def canalDataTest1(): Unit = {
        val updateRows = "{\"data\":[{\"c_card_license\":\"浙F282PY_0\",\"c_category\":\"1\",\"c_center_send_flag\":\"2\",\"c_en_license\":\"\",\"c_en_shift_code\":\"2\",\"c_en_vehicle_class\":\"1\",\"c_en_vehicle_type\":\"1\",\"c_lanetype\":\"3\",\"c_license\":\"浙F282PY_0\",\"c_license_color\":\"0\",\"c_mot_send_flag\":\"1\",\"c_part_vehicle_type\":\"1       \",\"c_special_type\":\"\",\"c_station_send_flag\":\"1\",\"c_temp1\":\"\",\"c_temp2\":\"\",\"c_terminal_number\":\"21330000CBAD\",\"c_terminal_tranno\":\"000569D3\",\"c_ticket_type\":\"5\",\"c_verifycode\":\"D9C41423\",\"d_enpermittedweight\":\"49.0\",\"d_enweight\":\"1.0\",\"d_trans_fee\":\"0.0\",\"n_algorithmidentifier\":\"1\",\"n_box_id\":\"20200812\",\"n_card_type\":\"23\",\"n_cpu_psamid\":\"52141\",\"n_direction\":\"0\",\"n_electricalpercent\":\"0\",\"n_en_card_id\":\"1906230102395155\",\"n_en_date\":\"20200812\",\"n_en_lane_id\":\"322971\",\"n_en_op_code\":\"3229999\",\"n_en_serial_no\":\"1301\",\"n_en_time\":\"123437\",\"n_enaxlecount\":\"2\",\"n_enspecialtruck\":\"0\",\"n_gantry_lines\":\"1\",\"n_obu_sign\":\"2\",\"n_psamid\":\"0\",\"n_sign_status\":\"1\",\"n_temp\":\"0\",\"n_temp1\":\"0\",\"n_temp2\":\"0\",\"n_trade_mode\":\"2\",\"n_trade_speed\":\"0.0\",\"n_trade_time\":\"580\",\"n_tranno\":\"1176\",\"n_trans_date\":\"20200812\",\"n_trans_time\":\"123438\",\"n_vehicle_class_gb\":\"0\",\"n_vehicle_seats\":\"2\",\"n_vehicle_sign\":\"255\",\"n_vehicle_usertype\":\"0\",\"n_zone_time\":\"12\",\"ts_center_send_time\":\"2020-08-12 12:34:50\",\"ts_timestamp\":\"2020-08-12 12:34:37\",\"vc_cardver\":\"41\",\"vc_cert_no\":\"\",\"vc_en_lane_hex\":\"3301201D47\",\"vc_enaxletype\":\"\",\"vc_enhex\":\"3301201D\",\"vc_issue_code\":\"3301\",\"vc_license_fix\":\"\",\"vc_obu_id\":\"3301131909323969\",\"vc_obumac\":\"0D864DCC\",\"vc_obusn\":\"3301131909323969\",\"vc_pass_id\":\"013301190623010239515520200812123437\",\"vc_special_type_gb\":\"\",\"vc_stationhex\":\"3301201D\",\"vc_tac\":\"65BA681C\",\"vc_trade_id\":\"S001233002001040101002020081212343701\",\"vc_vehicle_sign_id\":\"S001233002001040101002020081212001301\"}],\"database\":\"gstation\",\"dbNo\":101,\"es\":1597206890000,\"id\":138860,\"isDdl\":false,\"mysqlType\":{\"c_card_license\":\"varchar(20)\",\"c_category\":\"char(1)\",\"c_center_send_flag\":\"tinyint(4)\",\"c_en_license\":\"varchar(20)\",\"c_en_shift_code\":\"char(1)\",\"c_en_vehicle_class\":\"char(1)\",\"c_en_vehicle_type\":\"char(1)\",\"c_lanetype\":\"char(1)\",\"c_license\":\"varchar(20)\",\"c_license_color\":\"varchar(2)\",\"c_mot_send_flag\":\"tinyint(4)\",\"c_part_vehicle_type\":\"varchar(8)\",\"c_special_type\":\"char(8)\",\"c_station_send_flag\":\"char(1)\",\"c_temp\":\"char(10)\",\"c_temp1\":\"varchar(10)\",\"c_temp2\":\"varchar(10)\",\"c_terminal_number\":\"varchar(12)\",\"c_terminal_tranno\":\"varchar(12)\",\"c_ticket_type\":\"char(1)\",\"c_verifycode\":\"char(8)\",\"d_enpermittedweight\":\"decimal(8,2)\",\"d_enweight\":\"decimal(8,2)\",\"d_trans_fee\":\"decimal(8,2)\",\"n_algorithmidentifier\":\"int(6)\",\"n_box_id\":\"int(8)\",\"n_card_type\":\"int(8)\",\"n_cpu_psamid\":\"int(8)\",\"n_direction\":\"int(11)\",\"n_electricalpercent\":\"int(6)\",\"n_en_card_id\":\"varchar(16)\",\"n_en_date\":\"int(8)\",\"n_en_lane_id\":\"int(6)\",\"n_en_op_code\":\"int(8)\",\"n_en_serial_no\":\"int(6)\",\"n_en_time\":\"int(6)\",\"n_enaxlecount\":\"int(6)\",\"n_enspecialtruck\":\"int(6)\",\"n_gantry_lines\":\"tinyint(3) unsigned\",\"n_obu_sign\":\"int(6)\",\"n_psamid\":\"int(8)\",\"n_sign_status\":\"int(11)\",\"n_temp\":\"int(8)\",\"n_temp1\":\"int(8)\",\"n_temp2\":\"int(8)\",\"n_trade_mode\":\"tinyint(3) unsigned\",\"n_trade_speed\":\"decimal(8,2)\",\"n_trade_time\":\"int(8)\",\"n_tranno\":\"varchar(6)\",\"n_trans_date\":\"int(8)\",\"n_trans_time\":\"int(6)\",\"n_vehicle_class_gb\":\"int(10)\",\"n_vehicle_seats\":\"int(6)\",\"n_vehicle_sign\":\"int(10)\",\"n_vehicle_usertype\":\"int(10)\",\"n_zone_time\":\"int(2)\",\"ts_center_send_time\":\"datetime\",\"ts_mot_send_time\":\"datetime\",\"ts_timestamp\":\"datetime\",\"vc_cardver\":\"varchar(2)\",\"vc_cert_no\":\"varchar(50)\",\"vc_en_lane_hex\":\"varchar(10)\",\"vc_enaxletype\":\"varchar(6)\",\"vc_enhex\":\"varchar(10)\",\"vc_issue_code\":\"varchar(8)\",\"vc_license_fix\":\"varchar(20)\",\"vc_obu_id\":\"varchar(20)\",\"vc_obumac\":\"varchar(8)\",\"vc_obusn\":\"varchar(16)\",\"vc_pass_id\":\"varchar(40)\",\"vc_special_type_gb\":\"varchar(50)\",\"vc_stationhex\":\"varchar(8)\",\"vc_tac\":\"varchar(8)\",\"vc_trade_id\":\"varchar(38)\",\"vc_vehicle_sign_id\":\"varchar(200)\"},\"old\":[{\"c_center_send_flag\":\"1\"}],\"pkNames\":[\"vc_trade_id\"],\"sql\":\"\",\"sqlType\":{\"c_card_license\":12,\"c_category\":1,\"c_center_send_flag\":-6,\"c_en_license\":12,\"c_en_shif^Ct_code\":1,\"c_en_vehicle_class\":1,\"c_en_vehicle_type\":1,\"c_lanetype\":1,\"c_license\":12,\"c_license_color\":12,\"c_mot_send_flag\":-6,\"c_part_vehicle_type\":12,\"c_special_type\":1,\"c_station_send_flag\":1,\"c_temp\":1,\"c_temp1\":12,\"c_temp2\":12,\"c_terminal_number\":12,\"c_terminal_tranno\":12,\"c_ticket_type\":1,\"c_verifycode\":1,\"d_enpermittedweight\":3,\"d_enweight\":3,\"d_trans_fee\":3,\"n_algorithmidentifier\":4,\"n_box_id\":4,\"n_card_type\":4,\"n_cpu_psamid\":4,\"n_direction\":4,\"n_electricalpercent\":4,\"n_en_card_id\":12,\"n_en_date\":4,\"n_en_lane_id\":4,\"n_en_op_code\":4,\"n_en_serial_no\":4,\"n_en_time\":4,\"n_enaxlecount\":4,\"n_enspecialtruck\":4,\"n_gantry_lines\":-6,\"n_obu_sign\":4,\"n_psamid\":4,\"n_sign_status\":4,\"n_temp\":4,\"n_temp1\":4,\"n_temp2\":4,\"n_trade_mode\":-6,\"n_trade_speed\":3,\"n_trade_time\":4,\"n_tranno\":12,\"n_trans_date\":4,\"n_trans_time\":4,\"n_vehicle_class_gb\":4,\"n_vehicle_seats\":4,\"n_vehicle_sign\":4,\"n_vehicle_usertype\":4,\"n_zone_time\":4,\"ts_center_send_time\":93,\"ts_mot_send_time\":93,\"ts_timestamp\":93,\"vc_cardver\":12,\"vc_cert_no\":12,\"vc_en_lane_hex\":12,\"vc_enaxletype\":12,\"vc_enhex\":12,\"vc_issue_code\":12,\"vc_license_fix\":12,\"vc_obu_id\":12,\"vc_obumac\":12,\"vc_obusn\":12,\"vc_pass_id\":12,\"vc_special_type_gb\":12,\"vc_stationhex\":12,\"vc_tac\":12,\"vc_trade_id\":12,\"vc_vehicle_sign_id\":12},\"table\":\"entry_jour\",\"ts\":1597206890562,\"type\":\"UPDATE\"}"
        val config = StreamUtils.readConfig("local:/Volumes/workspace/zhy/pass_workspace/streamx/streamx-test/src/test/resources/data/${project.version}/mysql/mysql_stream_station_info.json")
        val streamxConf = reload(config)
        val rowsAb = ArrayBuffer.empty[Row]
        val parser = new CanalParser
        parser.parseRowWithCondition(updateRows, streamxConf.getConfigs.head.transformer.readerSchema, streamxConf.getConfigs.head.transformer.where, rowsAb)
        println(rowsAb)
    }

    @Test
    def test333(): Unit = {
        @transient lazy val mapper = new ObjectMapper()
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        val str = "{\"calling\":0,\"capTime\":1609910645468.0,\"carAttribute\":\"A\",\"carBrand\":\"0\",\"carColor\":\"0\",\"carColorA\":\"0\",\"carDirect\":\"2\",\"carHeight\":0,\"carLength\":0,\"carSpeed\":0,\"carType\":\"K33\",\"carTypeA\":\"K33\",\"carWayCode\":1,\"carWidth\":0,\"channelCode\":\"euhtIQnOA1C25ILASMN4MH\",\"channelId\":\"euhtIQnOA1C25ILASMN4MH\",\"country\":\"\",\"customCarDirect\":\"\",\"dataSource\":0,\"deviceId\":\"euhtIQnOA1C25ILASMN4GS\",\"dropCnt\":0,\"event\":\"trafficJunction\",\"gpsAngle\":0.0,\"gpsHeight\":0.0,\"gpsSpeed\":0.0,\"gpsX\":-180.0,\"gpsY\":-90.0,\"hasGps\":0,\"hasRack\":0,\"hasSparetire\":0,\"hasSunroff\":0,\"imageList\":[{\"cacheUrl\":\"33.53.1.189:48240/cameras/cache/picture?vcuid=euhtIQnOA1C25ILASMN4MH&key=8748126&node=10&time=1609910648\",\"imgHeight\":0,\"imgIdx\":1,\"imgPixel\":\"0*0\",\"imgSize\":788160,\"imgType\":0,\"imgUrl\":\"/image/efs_Adzev7nd_001/fd872a21c90be95f945b5ead_trafficJunction_36_2/archivefile1-2021-01-06-123920-4DFE91D30B043221:619773952/788160.jpg\",\"imgWidth\":0,\"parentImgIdx\":0},{\"cacheUrl\":\"\",\"imgHeight\":0,\"imgIdx\":1,\"imgPixel\":\"0*0\",\"imgSize\":0,\"imgType\":6,\"imgUrl\":\"\",\"imgWidth\":0,\"objBottom\":0,\"objLeft\":0,\"objRight\":0,\"objTop\":0,\"parentImgIdx\":1},{\"cacheUrl\":\"33.53.1.189:48240/cameras/cache/picture?vcuid=euhtIQnOA1C25ILASMN4MH&key=8748127&node=10&time=1609910648\",\"imgHeight\":80,\"imgIdx\":1,\"imgPixel\":\"240*80\",\"imgSize\":5233,\"imgType\":2,\"imgUrl\":\"/image/efs_Adzev7nd_001/fd872a21c90be95f945b5ead_plate_36_0/archivefile1-2021-01-05-160336-F31CA0560B043221:2052198400/5233.jpg\",\"imgWidth\":240,\"objBottom\":1141,\"objLeft\":2132,\"objRight\":2316,\"objTop\":1087,\"parentImgIdx\":1}],\"infoKind\":1,\"isDanger\":0,\"isDetectionSignFront\":0,\"isHangingsFront\":0,\"isPerfumeFront\":0,\"isSunVisorFront\":0,\"isSupplement\":0,\"lapbeltCnt\":2,\"lapbeltSupCnt\":2,\"mainSeatBelt\":0,\"maxSpeed\":70,\"minSpeed\":20,\"mobileCnt\":2,\"paperCnt\":0,\"picRecordId\":\"htIQnOA1C25ILASMN4MH0220210106052405053040263515\",\"plateColor\":\"0\",\"plateColorA\":\"0\",\"plateFlag\":\"\",\"plateNum\":\"浙K0E183\",\"plateNumA\":\"浙K0E183\",\"plateType\":\"02\",\"plateTypeA\":\"02\",\"recCode\":\"\",\"recType\":0,\"recordId\":\"htIQnOA1C25ILASMN4MH0220210106052405053040263515\",\"redLightDate\":0.0,\"regionCode\":0,\"safetyBelt\":0,\"slaveSeatBelt\":0,\"smokeCnt\":2,\"snapHeadstock\":0,\"sourceId\":\"htIQnOA1C25ILASMN4MH022021010605240505304\",\"storageFlag\":1,\"sunCnt\":0,\"tagCnt\":0,\"traceServiceInputTime\":1609910649242.0,\"traceThingsInputTime\":1609910648246.0,\"trafficLightState\":0,\"uid\":\"1\",\"userChannelCode\":\"euhtIQnOA1C25ILASMN4MH\",\"vehicleManufacturer\":\"0\",\"vehicleMode\":\"0\",\"vehicleStyle\":\"0\"}"
        val jsonNode = mapper.readTree(str)
        val node1 = jsonNode.get("carAttribute")
        val node2 = jsonNode.get("imageList")
        if(node1.isInstanceOf[TextNode])
            println(node1.asText())
        if(!node2.isInstanceOf[TextNode])
            println(node2.toString())
    }
}
