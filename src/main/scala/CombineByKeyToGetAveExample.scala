/**
  * Created by huangjing on 2017/8/1.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object CombineByKeyToGetAveExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("CombineByKeyToGetAveExample")
    val sc = new SparkContext(sparkConf)

    val pairRDD = sc.parallelize(List(("a"->2),("b"->4),("b"->6),("c"-> 3),("c"->4)))
    println(pairRDD.partitions.size)
    println(pairRDD.getNumPartitions)

    val resRDD = pairRDD.combineByKey((v) => (v,1),
      (acc:(Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1:(Int, Int), acc2:(Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

    //resRDD.map{(key, val) => (key, (val._1/val._2.toFloat))}


  }
}
