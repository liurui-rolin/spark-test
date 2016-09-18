package youling.studio.ad

import java.util.Properties
import java.text.SimpleDateFormat
import org.apache.spark.storage.StorageLevel
import youling.studio.utils.{RedisDao, JsonUtil}
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext,Duration}
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.{Row, SQLContext}
import org.json4s.jvalue2monadic
import org.apache.spark.sql.hive.HiveContext

import scala.collection.immutable.Map
import org.slf4j.LoggerFactory
import org.slf4j.Logger

/**
 *
 * 功能描述：
 *  实时统计广告推广相关数据
 */
object RealTimeStatAd {
  private val logger = LoggerFactory.getLogger(RealTimeStatAd.getClass)
  private var _properties = new Properties
  private val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")

  val WINDOW_LENGTH = new Duration(60 * 1000)
  val SLIDE_INTERVAL = new Duration(60 * 1000)
  val checkpointdir = "hdfs://nameservice1/user/dc/spark/checkpoint/"
  val ad_type = "ad_log"

  // 获取环境变量
  private def getProperties() {
    val inputStream = RealTimeStatAd.getClass().getClassLoader().getResourceAsStream("util.properties")
    this._properties.load(inputStream)
  }

  def incrByKey(line : Row)={
    val keys = List(line.getString(0),line.getString(1),line.getString(2),line.getString(3),line.getString(4),line.getString(5),line.getString(6))
    val key = keys.mkString(":")
    val cnt = line.getLong(7).toInt
    //logger.warn("----------------------:"+key+":"+cnt)
    RedisDao.getRedis().incrby(key,cnt)
  }

  def main(args: Array[String]) {
    // 初始化StreamingContext, 获取input Dstream
    this.getProperties
    val zkConnect = _properties.getProperty("zkConnect")
    val group = _properties.getProperty("group_ad")
    val topic = Map(_properties.getProperty("topic_ad") -> _properties.getProperty("partitions_ad").toInt)

    val sparkConf = new SparkConf().setAppName("RealTimeStatAd")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(60))
    ssc.checkpoint(checkpointdir)

    val lines1 = KafkaUtils.createStream(ssc, zkConnect, group, topic,StorageLevel.MEMORY_AND_DISK_SER).map(x=>JsonUtil.jsonParse(x._2)).filter(x=>(x \ "type").values.equals(ad_type)).map(AdTableSchema.parseLogLine(_))
    val lines2 = KafkaUtils.createStream(ssc, zkConnect, group, topic,StorageLevel.MEMORY_AND_DISK_SER).map(x=>JsonUtil.jsonParse(x._2)).filter(x=>(x \ "type").values.equals(ad_type)).map(AdTableSchema.parseLogLine(_))
    val lines = lines1.union(lines2).cache()

    val hiveContext = new HiveContext(sc)
    //计算每分钟的数据
    lines.foreachRDD(rdd => {
      if(rdd.count()==0){
        logger.info("there is no records!")
      }else{
        //val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        import hiveContext.implicits._

        val rddDF = rdd.toDF()
        rddDF.registerTempTable("ad_log")
        val results = hiveContext.sql("select time,query_type,new_os,new_source_type,new_appid,new_source,new_active,count(1) cnt from ( select time,query_type,Array('_NONE',os) os,Array('_NONE',source_type) source_type,Array('_NONE',appid) appid,Array('_NONE',source) source,Array('_NONE',active) active from ad_log ) t1 LATERAL VIEW EXPLODE(t1.os) a AS new_os LATERAL VIEW EXPLODE(t1.appid) a AS new_appid LATERAL VIEW EXPLODE(t1.source) a AS new_source LATERAL VIEW EXPLODE(t1.active) a AS new_active LATERAL VIEW EXPLODE(t1.source_type) a AS new_source_type group by time,query_type,new_os,new_source_type,new_appid,new_source,new_active")
        //results.show(200)
        logger.warn("ready for write to redis...")
        results.collect().map(incrByKey(_))
      }
    })

    // 启动计算
    ssc.start()
    ssc.awaitTermination()

  }

}
