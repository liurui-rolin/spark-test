package youling.studio.streaming

import org.apache.spark.streaming.{Seconds,StreamingContext}
import StreamingContext._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

/**
 * Created by rolin on 16/7/13.
 */
object SparkStreamingHA {
  def main (args: Array[String]){
    val conf = new SparkConf().setAppName("test socket streaming!")
    val checkpointdir = "./checkpoint"

    def createStreamingContext() = {
      val sc = new SparkContext(conf)
      val ssc = new StreamingContext(sc,Seconds(30))
      ssc.checkpoint(checkpointdir)
      ssc
    }
    val ssc = StreamingContext.getOrCreate(checkpointdir,createStreamingContext _)

    val lines = ssc.socketTextStream("127.0.0.1",9999,StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(line => line.split(" "))
    val wc = words.map((_,1)).reduceByKey((x,y)=> x+y)

    //wc.saveAsTextFile(output)
    wc.print

    ssc.start()
    ssc.awaitTermination()
  }
}
