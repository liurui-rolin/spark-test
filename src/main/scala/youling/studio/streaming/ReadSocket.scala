package youling.studio.streaming

import org.apache.spark.streaming.{Seconds,StreamingContext}
import StreamingContext._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

/**
 * Created by rolin on 16/7/11.
 */
object ReadSocket {
  def main (args: Array[String]){
    if(args.length<2){
      System.err.println("需要两个参数.")
      System.exit(1)
    }


    val Array(master,output) = args.take(2)
    System.out.println(master)
    System.out.println(output)

    val conf = new SparkConf().setMaster(master).setAppName("test socket streaming!")
    val ssc = new StreamingContext(conf,Seconds(30))

    val lines = ssc.socketTextStream("127.0.0.1",7777,StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(line => line.split(" "))
    val wc = words.map((_,1)).reduceByKey((x,y)=> x+y)

    //wc.saveAsTextFile(output)
    wc.print

    println("start streaming")
    ssc.start()
    ssc.awaitTermination()
    println("done")
  }
}
