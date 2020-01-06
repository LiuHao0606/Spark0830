package com.atguigu.spark.core.day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object FlatMapRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("FlatMapRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(10, 60, 70, 50, 65, 35, 25, 12))
    //val rdd2: RDD[Int] = rdd1.flatMap(x => Array(x, x * x, x * x * x))

    //val rdd2Char: RDD[Char] = rdd1.flatMap(x => x + "")
    //println(rdd2Char.collect.mkString(","))

    //使用flatMap实现过滤
    val rdd2: RDD[Int] = rdd1.flatMap(x => if (x > 50) Array(x) else Nil)

    println(rdd2.collect.mkString(","))

    sc.stop()
  }
}
