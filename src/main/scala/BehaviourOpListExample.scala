/**
  * Created by huangjing on 2017/7/27.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object BehaviourOpListExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("BehaviourOpListExample")
    val sc = new SparkContext(sparkConf)

    val inRDD = sc.parallelize(List(1,2,3,3))

    //1. collect
    inRDD.collect().foreach(println)

    //2. count
    println(inRDD.count())

    //3. take
    inRDD.take(2).foreach(println)

    //4. top:default descend
    inRDD.top(2).foreach(println)

    //5. takeOrdered:default ascend
    inRDD.takeOrdered(2).foreach(println)

    // reverse order
    implicit val myOrd = implicitly[Ordering[Int]].reverse
    println(inRDD.top(2)(myOrd).mkString("<="))
    println(inRDD.takeOrdered(2)(myOrd).mkString(">="))

    //6. takeSample
    inRDD.takeSample(false,3).foreach(println)
    inRDD.takeSample(false,3,100).foreach(println)
    inRDD.takeSample(true,5).foreach(println)

    //7. reduce
    println(inRDD.reduce((x,y)=>x+y)) //sum

    //8. fold
    println(inRDD.fold(1)((x,y)=>x+y)) //sum with initial number(2)

    //9. aggregate
    val Rs =inRDD.aggregate((0.0,0))(
                (x,value) => (x._1 + value, x._2 + 1),
                (sum1,sum2) => (sum1._1 + sum2._1, sum1._2 + sum2._2))
    println(Rs._1/Rs._2)

    //10. foreach
    inRDD.foreach(println)

  }

}
