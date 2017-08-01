/**
  * Created by huangjing on 2017/8/1.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object mapAndReduceToGetAveExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("mapAndReduceToGetAveExample")
    val sc = new SparkContext(sparkConf)

    val pairRDD = sc.parallelize(List(("a"->2),("b"->4),("b"->6)))

    val keySumRDD = pairRDD.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
    val keyAveRDD = keySumRDD.mapValues(v => v._1/(v._2*1.0))

    keyAveRDD.foreach(println)

  }

}
