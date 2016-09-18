package youling.studio

import org.apache.spark.{SparkFiles, SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

/**
 * Created by rolin on 16/5/11.
 */
class SparkWC {
  def wc() : Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("word count")
    val sc = new SparkContext(conf)
    val input = sc.textFile("/Users/rolin/newUserAndroid.php")
    val words = input.flatMap(line=>line.split(" "))
    val count = words.map(word=>(word,1)).reduceByKey{case (x,y) => x + y}
    count.saveAsTextFile("/Users/rolin/result_001.txt")
  }
}

