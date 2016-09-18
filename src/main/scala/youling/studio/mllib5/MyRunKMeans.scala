package youling.studio.mllib5

import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by rolin on 16/8/18.
 */
object MyRunKMeans {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("K-means"))
    val rawData = sc.textFile("hdfs://nameservice1/user/dc/spark/kddcup/kddcup.data",6)
    rawData.cache()
    clusteringTake0(rawData)
    clusteringTake1(rawData)
    clusteringTake2(rawData)

    clusteringTake3(rawData)

    clusteringTake4(rawData)

    rawData.unpersist()
  }

  def entropy(counts : Iterable[Int]) = {
    val values = counts.filter(_>0)
    val n : Double = values.sum
    values.map{ v=>
      val p=v/n
      -p*math.log(p)
    }.sum
  }
  def clusteringScore3(normalizedLableAndData:RDD[(String,Vector)],k:Int):Double = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)

    val model = kmeans.run(normalizedLableAndData.values)

    val labelAndClusters = normalizedLableAndData.mapValues(model.predict)
    val clustersAndLabels = labelAndClusters.map(_.swap)
    val labelsInCluster = clustersAndLabels.groupByKey().values
    val labelCounts = labelsInCluster.map(_.groupBy(l=>l).map(_._2.size))
    val n = normalizedLableAndData.count()
    labelCounts.map(m=>m.sum*entropy(m)).sum/n
  }

  def clusteringTake4(rawData: RDD[String]): Unit = {
    val parseFunction = buildCategoricalAndLabelFunction(rawData)
    val labelsAndData = rawData.map(parseFunction)
    val normalizedLableAndData = labelsAndData.mapValues(buildNormalizationFunction(labelsAndData.values)).cache()

    (80 to 160 by 10).map(k => (k,clusteringScore3(normalizedLableAndData,k))).toList.foreach(println)

    normalizedLableAndData.unpersist()
  }

  def buildCategoricalAndLabelFunction(rawData:RDD[String]):(String=>(String,Vector))={
    val splitData = rawData.map(_.split(','))
    val protocols = splitData.map(_(1)).distinct().collect().zipWithIndex.toMap
    val services = splitData.map(_(2)).distinct().collect().zipWithIndex.toMap
    val tcpstates = splitData.map(_(3)).distinct().collect().zipWithIndex.toMap
    (line:String)=>{
      val buffer = line.split(',').toBuffer
      val protocol = buffer.remove(1)
      val service = buffer.remove(1)
      val tcpState = buffer.remove(1)
      val label = buffer.remove(buffer.length - 1)
      val vector = buffer.map(_.toDouble)

      val newProtocolFeatures = new Array[Double](protocols.size)
      newProtocolFeatures(protocols(protocol)) = 1.0
      val newServiceFeatures = new Array[Double](services.size)
      newServiceFeatures(services(service)) = 1.0
      val newTcpStateFeatures = new Array[Double](tcpstates.size)
      newTcpStateFeatures(tcpstates(tcpState)) = 1.0

      vector.insertAll(1,newTcpStateFeatures)
      vector.insertAll(1,newServiceFeatures)
      vector.insertAll(1,newProtocolFeatures)

      (label,Vectors.dense(vector.toArray))
    }
  }

  def clusteringTake3(rawData: RDD[String]): Unit = {
    val parseFunction = buildCategoricalAndLabelFunction(rawData)
    val data = rawData.map(parseFunction(_)).values
    val normalizedData = data.map(buildNormalizationFunction(data)(_)).cache()

    (80 to 160 by 10).map(k=> (k,clusteringScore2(normalizedData,k))).toList.foreach(println)

    normalizedData.unpersist()
  }

  def buildNormalizationFunction(data: RDD[Vector]): (Vector => Vector) = {
    val dataAsArray = data.map(_.toArray)
    val numCols = dataAsArray.first().length
    val n = dataAsArray.count()
    val sums = dataAsArray.reduce{case (a,b) => a.zip(b).map(t=>t._1+t._2) }
    val sumSquares = dataAsArray.aggregate(
      new Array[Double](numCols)
    )(
      (a, b) => a.zip(b).map(t => t._1 + t._2 * t._2),
      (a,b) => a.zip(b).map(t=>t._1 + t._2)
    )
    val stdevs = sumSquares.zip(sums).map{
      case (sumSq,sum)=>math.sqrt(n*sumSq-sum*sum)/n
    }
    val means = sums.map(_/n)

    (datum:Vector)=>{
      val normalizedArray = (datum.toArray,means,stdevs).zipped.map(
        (value,mean,stdev) =>
          if ( stdev <= 0 ) (value-mean) else (value - mean) / stdev
      )
      Vectors.dense(normalizedArray)
    }

  }

  def clusteringScore2(normalizedData:RDD[Vector],k : Int): Unit ={
    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(normalizedData)
    normalizedData.map(datum => distToCentroid(datum,model))
  }

  def clusteringTake2(rawData : RDD[String]): Unit ={
    val data = rawData.map{line=>
      val buffer = line.split(',').toBuffer
      buffer.remove(1,3)
      buffer.remove(buffer.length-1)
      Vectors.dense(buffer.map(_.toDouble).toArray)
    }

    val normalizedData = data.map(buildNormalizationFunction(data)).cache()

    (60 to 120 by 10).par.map(k=>
      (k,clusteringScore2(normalizedData,k))
    ).toList.foreach(println)

    normalizedData.unpersist()
  }

  def visualizationInR(rawData:RDD[String]): Unit ={
    val data = rawData.map{line=>
      val buffer = line.split(',').toBuffer
      buffer.remove(1,3)
      buffer.remove(buffer.length-1)
      Vectors.dense(buffer.map(_.toDouble).toArray)
    }.cache()

    val kmeans = new KMeans()
    kmeans.setK(100)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(data)

    val sample = data.map(datum=>
      model.predict(datum)+","+datum.toArray.mkString(",")
    ).sample(false,0.05)

    sample.saveAsTextFile("hdfs://nameservice1/user/dc/spark/kddcup/sample_kddcup.data")

    data.unpersist()

  }


  def distance(a:Vector,b:Vector) = {
    math.sqrt(a.toArray.zip(b.toArray).map(p => p._1 - p._2).map(d => d * d).sum)
  }

  def distToCentroid(datum:Vector,model :KMeansModel) : Double = {
    val cluster = model.predict(datum)
    val centroid = model.clusterCenters(cluster)
    distance(datum,centroid)
  }

  def clusteringScore(data : RDD[Vector],k : Int) : Double = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    val model = kmeans.run(data)
    data.map(datum=>distToCentroid(datum,model)).mean()
  }

  def clusteringTake1(rawData:RDD[String]): Unit ={
    val data = rawData.map{ line=>
      val buffer = line.split(',').toBuffer
      buffer.remove(1,3)
      buffer.remove(buffer.length-1)
      Vectors.dense(buffer.map(_.toDouble).toArray)
    }.cache()

    (5 to 30 by 5).map(k=>(k,clusteringScore(data,k))).foreach(println)

    (30 to 100 by 10).par.map(k=>(k,clusteringScore(data,k))).toList.foreach(println)

    data.unpersist()

  }


  def clusteringTake0(rawData:RDD[String]): Unit ={
    rawData.map(_.split(',').last).countByValue().toSeq.sortBy(_._2).reverse.map(println)

    val labelAndData = rawData.map{ line=>
      val buffer = line.split(',').toBuffer
      buffer.remove(1,3)
      val label = buffer.remove(buffer.length-1)
      val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
      (label,vector)
    }

    val data = labelAndData.values.cache()

    val kmeans = new KMeans()
    val model = kmeans.run(data)

    model.clusterCenters.foreach(println)

    val clusterLabelCount = labelAndData.map{case (label,datum)=>
      val cluster = model.predict(datum)
      (cluster,label)
    }.countByValue()

    clusterLabelCount.toSeq.sorted.foreach{ case((cluster,label),count) =>
      println(f"$cluster%1s$label%18s$count%8s")
    }

    data.unpersist()
  }
}
