package youling.studio.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{StreamingContext, Duration,Seconds}
import StreamingContext._

/**
 * Created by rolin on 16/7/13.
 */
object StreamingJoin {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test join!")
    val ssc = new StreamingContext(conf,Seconds(10))

    //检查点
    ssc.checkpoint("/Users/rolin/soft/spark-1.1.0-bin-hadoop2.4/checkpoint")

    val lines = ssc.socketTextStream("localhost",9999,StorageLevel.MEMORY_ONLY)
    val accessLog : DStream[ApacheAccessLog] = lines.map(line => ApacheAccessLog.parseLogLine(line))

    //测试updateStateByKey
    def updateRunningSum(values : Seq[Long],state : Option[Long])={
      Some(state.getOrElse(0L)+values.size)
    }
    val resCodeDS = accessLog.map(entry=>(entry.responseCode,1L))
    val resCodeCountDS = resCodeDS.updateStateByKey(updateRunningSum _)
    resCodeCountDS.print()

    //测试join
    /*
    //ip数
    val ipDS = accessLog.map(entry => (entry.getIpAddress(),1))
    val ipCountDS = ipDS.reduceByKey((x,y)=>x+y)

    //size
    val ipSizeDS = accessLog.map(entry=>(entry.ipAddress,entry.contentSize))
    val ipSizeSumDS = ipSizeDS.reduceByKey((x,y)=>x+y)

    //join
    val sumDS = ipSizeSumDS.join(ipCountDS)
    //sumDS.print()
    */

    //测试reduceByKeyAndWindow
    //val ipWindowCountDS = ipDS.reduceByKeyAndWindow(
    //{(x,y)=>x+y},
    //{(x,y)=>x-y},
    //Seconds(30),Seconds(20))
    //ipWindowCountDS.print()

    //测试countByValueAndWindow
    //val ipDS1 = accessLog.map(entry=>entry.ipAddress)
    //val ipAddressReqCount = ipDS1.countByValueAndWindow(Seconds(30),Seconds(20))
    //val reqCount = accessLog.countByWindow(Seconds(30),Seconds(20))
    //ipAddressReqCount.print()
    //reqCount.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
