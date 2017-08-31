/**
  * Created by huangjing on 2017/8/31.
  */

import org.apache.spark.{SparkConf, SparkContext}

object ActionOpOnPairRDDExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("ActionOpOnPairRDDExample")
    val sc = new SparkContext(sparkConf)

    // actions on a single pair RDD
    val pairRDD = sc.parallelize(List((1 -> 2), (3 -> 4), (3 -> 6)))
    pairRDD.foreach(println)

    println("countBykey -----------")
    pairRDD.countByKey.foreach(println)

    println("collect as Map -----------")
    //this doesn't return a multimap (so if you have multiple values to the same key, only one value per key is preserved in the map returned)
    pairRDD.collectAsMap.foreach(println)

    println("look up-----------")
    pairRDD.lookup(3).foreach(println)

  }
}
