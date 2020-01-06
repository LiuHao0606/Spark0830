package com.atguigu.spark.core.MyTest01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Test01").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8),4)
    val rdd2: RDD[(Int, Int)] = rdd1.mapPartitionsWithIndex((index, it) => it.map((index, _)))
    println(rdd2.collect.mkString(","))



  }
}
