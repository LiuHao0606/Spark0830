package com.atguigu.spark.core.day02.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Pratice {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Pratice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //val rdd1: RDD[String] = sc.textFile("C:\\Users\\LH\\Desktop\\2019_08_30\\01_spark\\02_资料\\agent.log")
    val rdd1: RDD[String] = sc.textFile(Pratice.getClass.getClassLoader.
      getResource("agent.log").getFile)

    val rdd2: RDD[((String, String), Int)] = rdd1.map(s => {
      val strings: Array[String] = s.split(" ")
      ((strings(1), strings(4)), 1)
    })
    val rdd3: RDD[((String, String), Int)] = rdd2.reduceByKey(_ + _)
    val rdd4: RDD[(String, Iterable[(String, Int)])] = rdd3.map {
      case ((pro, ads), count) => (pro, (ads, count))
    }.groupByKey()
    val rdd5: RDD[(String, List[(String, Int)])] = rdd4.map {
      case (pro, it) => (pro, it.toList.sortBy(s => s._2)(Ordering.Int.reverse).take(3))
    }
    val rdd6: RDD[(String, List[(String, Int)])] = rdd5.sortByKey()

    rdd6.collect().foreach(println)
  }
}
