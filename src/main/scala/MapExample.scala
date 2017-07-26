/**
  * Created by huangjing on 2017/7/26.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object MapExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("MapExample")
    val sc = new SparkContext(sparkConf)

    val inRdd = sc.parallelize(List(1,2,3,4))
    val mapRDD = inRdd.map(x => x*x)

    println(mapRDD.collect().mkString(","))
  }
}
