/**
  * Created by huangjing on 2017/7/28.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCountExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("MapExample")
    val sc = new SparkContext(sparkConf)

    val inFileRDD = sc.textFile("../MyFirstSparkLearn/src/main/resources/someSentence.txt")

    // compute the number of lines and show some sentence
    val lines = inFileRDD.count()
    val firstSentence = inFileRDD.first()

    println("this file has " + lines + " sentences, and the first sentence is: " + firstSentence)

    val wordsRDD = inFileRDD.flatMap(line => line.split(" "))
    val setWordInitNumRDD = wordsRDD.map(x => (x,1))
    val wordCounts = setWordInitNumRDD.reduceByKey((x,y) => x + y)

    wordCounts.saveAsTextFile("../MyFirstSparkLearn/src/main/resources/someSentence_WordCountRs")
  }


}
