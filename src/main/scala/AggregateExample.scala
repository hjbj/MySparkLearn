/**
  * Created by huangjing on 2017/7/27.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object AggregateExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("AggregateExample")
    val sc = new SparkContext(sparkConf)

    val inRdd = sc.parallelize(List(1.0,2,3,4),2)
    val sumCount = inRdd.aggregate((0.0,0))(
                        (acc,value) => (acc._1+value,acc._2+1),
                        (acc1,acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

    val ave = sumCount._1 / sumCount._2

    println(ave)
  }
}
