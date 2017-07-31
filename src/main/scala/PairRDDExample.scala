/**
  * Created by huangjing on 2017/7/31.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object PairRDDExample {
  def main(args: Array[String]): Unit = {
    val sparkConf= new SparkConf().setMaster("local").setAppName("PairRDDExample")
    val sc = new SparkContext(sparkConf)

    val inRDD = sc.textFile("../MyFirstSparkLearn/src/main/resources/someSentence.txt")

    //take the first word for the key of the sentence, transform RDD to pair PDD: (key, value)
    val firstWordRDD = inRDD.map(line => (line.split(" ")(0), line))

    firstWordRDD.foreach(println)

  }
}
