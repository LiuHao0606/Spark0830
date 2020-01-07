package com.atguigu.spark.core.day02.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TakeOrdered {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("TakeOrdered").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(10, 60, 70, 50,60))
    val rdd2: Array[Int] = rdd1.takeOrdered(3)(Ordering.Int.reverse)
    rdd2.foreach(println)
    sc.stop()
  }
}
