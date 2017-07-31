/**
  * Created by huangjing on 2017/7/31.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object BehaviourOpOnPairRDDExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("BehaviourOpOnPairRDDExample")
    val sc = new SparkContext(sparkConf)

    val pairRDD = sc.parallelize(List((1->2),(3->4),(3->6)))
    pairRDD.foreach(println)

    val reduceRDD = pairRDD.reduceByKey((x,y) => (x+y))
    reduceRDD.foreach(println)

    val groupRDD = pairRDD.groupByKey
    groupRDD.foreach(println)

    val combineRDD = false

    val mapRDD = pairRDD.mapValues(x => x+1)
    mapRDD.foreach(println)

    val flatMapRDD = pairRDD.flatMapValues((x => x to 5))
    flatMapRDD.foreach(println)

    val keyRDD = pairRDD.keys
    keyRDD.foreach(println)

    val valueRDD = pairRDD.values
    valueRDD.foreach(println)

    // default is ascend order, using 'false' to change the order to descend
    val sortRDD = pairRDD.sortByKey(false)
    sortRDD.foreach(println)

  }
}
