package youling.studio

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * Created by rolin on 16/5/19.
 */
class SparkPartition {

}

/*
object SparkMain{
  val conf = new SparkConf().setMaster("local").setAppName("test")
  val sc = new SparkContext(conf)
  val userData = sc.textFile("/Users/rolin/Downloads/aaa.txt").map(x=>(x.split("\t")(1),x.split("\t")(2))).persist()

  def processFind(file : String): Unit ={
    val events = sc.textFile("/Users/rolin/Downloads/bbb.txt").map(x=>(x.split("\t")(1),x.split("\t")(2)))
    val joined = userData.join(events)
    val filterRdd = joined.filter{
      case (userId,(userInfo,linkInfo)) =>
        !userInfo.toString().split(",").contains(linkInfo)
    }
  }

  def main (args: Array[String]){
    new HashPartitioner()

  }
}
*/


