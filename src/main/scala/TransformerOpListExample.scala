/**
  * Created by huangjing on 2017/7/28.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object TransformerOpListExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("TransformerOpListExample")
    val sc = new SparkContext(sparkConf)

    val inRDD = sc.parallelize(List(1,2,3,4))


    //1.Map
    val mapRDD = inRDD.map(x => x*x)
    mapRDD.foreach(println)

    //2.Filter
    val filterRDD = inRDD.filter(x => x%2==0)
    filterRDD.foreach(println)

    //3.FlatMap
    val wordRDD = sc.parallelize(List("hello world", "mike"))
    val wordListRDD = wordRDD.flatMap(line => line.split(" "))
    println(wordListRDD.first())
    println(wordListRDD.collect().mkString(","))




  }


}
