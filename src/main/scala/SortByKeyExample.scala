/**
  * Created by huangjing on 2017/8/31.
  */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SortByKeyExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("SortByKeyExample")
    val sc = new SparkContext(conf)
    println(sc.master)

    val pairRDD = sc.parallelize(List(("a"->2),("b"->4),("b"->6),("z" -> 1)))

    pairRDD.sortByKey().foreach(println)

    pairRDD.sortByKey(false).foreach(println)
  }

}
