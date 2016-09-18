package youling.studio.mllib

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD

/**
 * Created by rolin on 16/7/14.
 */
object MailClass {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("邮件分类!")
    val sc = new SparkContext(conf)

    val spam = sc.textFile("spam.txt")
    val normal = sc.textFile("normal.txt")

    val tf = new HashingTF(numFeatures = 10000)

    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
    val normalFeatures = normal.map(email => tf.transform(email.split(" ")))

    val positiveExamples = spamFeatures.map(feature => LabeledPoint(1,feature))
    val negativeExamples = normalFeatures.map(feature => LabeledPoint(0,feature))
    val trainDate = positiveExamples.union(negativeExamples)
    trainDate.cache()

    val model = new LogisticRegressionWithSGD().run(trainDate)

    val posTestExample = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))
    val negTestExample = tf.transform("Hi Dad, I started studying Spark the other ...".split(" "))

    println("Prediction for positive  test example : " + model.predict(posTestExample))
    println("Prediction for negative  test example : " + model.predict(negTestExample))

  }
}
