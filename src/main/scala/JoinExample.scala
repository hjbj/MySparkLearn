/**
  * Created by huangjing on 2017/8/31.
  */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object JoinExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("JoinExample")
    val sc = new SparkContext(sparkConf)

    val storedAddress = sc.parallelize(Seq(
      ("Ritual", "1026 Valencia st"),
      ("Philz", "748 Van Ness Ave"),
      ("Philz", "3101 24th St"),
      ("Starbucks", "Seattle")
    ))

    val storeRating = sc.parallelize(Seq(
      ("Ritual", 4.9),
      ("Philz", 4.8),
      ("UR", 5.1)
    ))

    storedAddress.join(storeRating).collect().foreach(println)

    storedAddress.leftOuterJoin(storeRating).foreach(println)

    storedAddress.rightOuterJoin(storeRating).foreach(println)
  }

}
