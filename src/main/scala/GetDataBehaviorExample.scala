/**
  * Created by huangjing on 2017/7/27.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object GetDataBehaviorExampleExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("GetDataBehaviorExampleExample")
    val sc = new SparkContext(sparkConf)

    val inRdd = sc.parallelize(List(1.0,2,3,4,5,6,7,8,9,10,11,12,13,14,15),4)


    println("collect() will put all data into memory(if your have few data)")
    println(inRdd.collect().mkString(","))

    println("top(n) will get the top n data(if your data is in order,you can choose this way to display)")
    inRdd.top(5).foreach(print)

    println("take(n) will get the n data from sub-partition as few as possible")
    inRdd.take(5).foreach(print)

    println("sample the data")
    inRdd.takeSample(false,5,100).foreach(println)

  }
}
