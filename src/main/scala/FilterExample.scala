/**
  * Created by huangjing on 2017/7/26.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object FilterExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("FilterExample")
    val sc = new SparkContext(sparkConf)

    val inRDD = sc.parallelize(List(1,2,3,4))
    val filterRDD = inRDD.filter(x => x%2==0)

    println(filterRDD.collect().mkString(","))

  }
}
