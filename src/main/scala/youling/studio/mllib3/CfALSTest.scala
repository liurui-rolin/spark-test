package youling.studio.mllib3

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * Created by rolin on 16/8/1.
 */
object CfALSTest {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Recommender"))
    val base = "hdfs://nameservice1/user/dc/spark/cflog/"
    val rawUserArtistData = sc.textFile(base + "user_artist_data.txt")
    val rawArtistData = sc.textFile(base + "artist_data.txt")
    val rawArtistAlias = sc.textFile(base + "artist_alias.txt")

    //evaluate(sc, rawUserArtistData, rawArtistAlias)
    recommend(sc,rawUserArtistData,rawArtistData,rawArtistAlias)

  }

  def recommend(sc:SparkContext,rawUserArtistData:RDD[String],rawArtistData:RDD[String],rawArtistAlias:RDD[String]): Unit ={
    val bArtistAlias = sc.broadcast(buildArtistAlisa(rawArtistAlias))
    val allData = buildRatings(rawUserArtistData,bArtistAlias).cache()
    val model = ALS.trainImplicit(allData,50,10,1.0,40.0)
    allData.unpersist()

    val userID = 2093760
    val recommendations = model.recommendProducts(userID,5)
    val recommendedProductIDs = recommendations.map(_.product).toSet

    val artistByID = buildArtistByID(rawArtistData)

    artistByID.filter{ case (id,name) => recommendedProductIDs.contains(id)}.values.collect().foreach(println)

    val someUsers = allData.map(_.user).distinct().take(100)
    val someRecommendations = someUsers.map(userID=>model.recommendProducts(userID,5))
    someRecommendations.map(
      recs => recs.head.user+"->"+recs.map(_.product).mkString(",")
    ).foreach(println)

    unpersist(model)
  }

  def buildArtistByID(rawArtistData:RDD[String]) = {
    rawArtistData.flatMap{ line=>
      val (id,name) = line.span(_ != '\t')
      if(name.isEmpty){
        None
      }else{
        try{
          Some(id.toInt,name.trim)
        }catch{
          case e:NumberFormatException=>None
        }
      }
    }
  }

  def evaluate(sc: SparkContext, rawUserArtistData: RDD[String], rawArtistAlias: RDD[String]): Unit = {
    val bArtistAlias = sc.broadcast(buildArtistAlisa(rawArtistAlias))

    val allData = buildRatings(rawUserArtistData, bArtistAlias)
    val Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0.1))
    trainData.cache()
    cvData.cache()

    val allItemIDs = allData.map(_.product).distinct().collect()
    val bAllItemIDs = sc.broadcast(allItemIDs)

    val mostListenedAUC = areaUnderCurve(cvData, bAllItemIDs, predictMostListened(sc, trainData))
    println(mostListenedAUC)

    val evaluations =
      for (rank <- Array(10, 50);
           lambda <- Array(1.0, 0.0001);
           alpha <- Array(1.0, 40.0))
        yield {
          val model = ALS.trainImplicit(trainData, rank, 10, lambda, alpha)
          val auc = areaUnderCurve(cvData, bAllItemIDs, model.predict)
          unpersist(model)
          ((rank, lambda, alpha), auc)
        }

    evaluations.sortBy(_._2).reverse.foreach(println)

    trainData.unpersist()
    cvData.unpersist()

  }

  def unpersist(model: MatrixFactorizationModel): Unit = {
    model.userFeatures.unpersist()
    model.productFeatures.unpersist()
  }

  def predictMostListened(sc: SparkContext, trainData: RDD[Rating])(allData: RDD[(Int, Int)]) = {
    val bListenCount = sc.broadcast(trainData.map(r => (r.product, r.rating)).reduceByKey(_ + _).collectAsMap())
    allData.map { case (user, product) =>
      Rating(user, product, bListenCount.value.getOrElse(product, 0.0))
    }
  }

  def areaUnderCurve(
                      positiveData: RDD[Rating],
                      bAllItemIDs: Broadcast[Array[Int]],
                      predictFunction: (RDD[(Int, Int)] => RDD[Rating])) = {
    // What this actually computes is AUC, per user. The result is actually something
    // that might be called "mean AUC".

    // Take held-out data as the "positive", and map to tuples
    val positiveUserProducts = positiveData.map(r => (r.user, r.product))
    // Make predictions for each of them, including a numeric score, and gather by user
    val positivePredictions = predictFunction(positiveUserProducts).groupBy(_.user)

    // BinaryClassificationMetrics.areaUnderROC is not used here since there are really lots of
    // small AUC problems, and it would be inefficient, when a direct computation is available.

    // Create a set of "negative" products for each user. These are randomly chosen
    // from among all of the other items, excluding those that are "positive" for the user.
    val negativeUserProducts = positiveUserProducts.groupByKey().mapPartitions {
      // mapPartitions operates on many (user,positive-items) pairs at once
      userIDAndPosItemIDs => {
        // Init an RNG and the item IDs set once for partition
        val random = new Random()
        val allItemIDs = bAllItemIDs.value
        userIDAndPosItemIDs.map { case (userID, posItemIDs) =>
          val posItemIDSet = posItemIDs.toSet
          val negative = new ArrayBuffer[Int]()
          var i = 0
          // Keep about as many negative examples per user as positive.
          // Duplicates are OK
          while (i < allItemIDs.size && negative.size < posItemIDSet.size) {
            val itemID = allItemIDs(random.nextInt(allItemIDs.size))
            if (!posItemIDSet.contains(itemID)) {
              negative += itemID
            }
            i += 1
          }
          // Result is a collection of (user,negative-item) tuples
          negative.map(itemID => (userID, itemID))
        }
      }
    }.flatMap(t => t)
    // flatMap breaks the collections above down into one big set of tuples

    // Make predictions on the rest:
    val negativePredictions = predictFunction(negativeUserProducts).groupBy(_.user)

    // Join positive and negative by user
    positivePredictions.join(negativePredictions).values.map {
      case (positiveRatings, negativeRatings) =>
        // AUC may be viewed as the probability that a random positive item scores
        // higher than a random negative one. Here the proportion of all positive-negative
        // pairs that are correctly ranked is computed. The result is equal to the AUC metric.
        var correct = 0L
        var total = 0L
        // For each pairing,
        for (positive <- positiveRatings;
             negative <- negativeRatings) {
          // Count the correctly-ranked pairs
          if (positive.rating > negative.rating) {
            correct += 1
          }
          total += 1
        }
        // Return AUC: fraction of pairs ranked correctly
        correct.toDouble / total
    }.mean() // Return mean AUC over users
  }

  def buildRatings(rawUserArtistData: RDD[String], bArtistAlias: Broadcast[Map[Int, Int]]) = {
    rawUserArtistData.map { line =>
      val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
      Rating(userID, finalArtistID, count)
    }
  }

  def buildArtistAlisa(rawArtistAlias: RDD[String]): Map[Int, Int] =
    rawArtistAlias.flatMap { line =>
      val tokens = line.split('\t')
      if (tokens(0).isEmpty) {
        None
      } else {
        Some((tokens(0).toInt, tokens(1).toInt))
      }
    }.collectAsMap()


}
