package com.atguigu.spark.core.day02.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Reduce {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Reduce").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(10, 60, 70, 50,60,80))

    val sum: Long = rdd1.count()
    println(sum)
    sc.stop()
  }
}
