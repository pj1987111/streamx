package cn.tongdun.streamx.other

import org.apache.spark.sql.SparkSession
import org.junit.{Before, Test}

/**
  * @Author hongyi.zhou
  * @Description
  * @Date create in 2020-04-27. 
  **/
class SparkJobTest {
    var sparkSession: SparkSession = _

    @Before
    def setUp() = {
        //设置读取的用户，不设置的话默认是本机登录用户
        System.setProperty("HADOOP_USER_NAME", "admin")
        //读取hive数据只需要设定hive.metastore.uris，然后把core-site.xml,hdfs-site.xml和hive-site.xml放到环境变量下即可
        sparkSession = SparkSession.builder()
                .master("local[8]")
                .appName("testtest123456")
                .config("hive.metastore.uris", "thrift://33.69.6.23:9083")
                .enableHiveSupport()
                .getOrCreate()
    }

    @Test
    def test1() = {
//        val gatherTime : String = sparkSession.sparkContext.getConf.get("gatherTime")
//        logger.info(String.format("Run dba statistic job to gather count hourly, gatherTime: %s.",gatherTime))
//        val endTime : Date = new SimpleDateFormat("yyyyMMdd HH:00:00.000").parse(gatherTime)
//        val startInstant = endTime.toInstant().minus(1, ChronoUnit.HOURS)
//        val startTime = new Date(startInstant.toEpochMilli)

//        logger.info(String.format("startTime: %s , endTime: %s.",startTime,endTime));

        /*        val sqlLtls = s"SELECT device_id, plate" +
                  " FROM dba_long_time_low_speed_record_mt" +
                  " WHERE time >= $startTime and time < $endTime"

                val resultLtls = sparkSession.sql(sqlLtls).collectAsList()
                logger.info("Result: " + resultLtls)*/

        val resultLtls2 = sparkSession.table("hhy_dw.check_health_dt")
                /*          .filter(r =>  {
                              val time = r.getAs("curgantrytime")
                              val curTime : Date = new SimpleDateFormat("yyyyMMdd HH:mm:ss").parse(time)
                              if (startTime.before(curTime) && endTime.after(curTime)) return true
                              return false
                          })*/
                .select("checktime","data")
                .limit(10)
                .collectAsList()

        val resultLtls3 = sparkSession.sql("select * from hhy_dw.check_health_dt")
        resultLtls3.show()

    }
}
