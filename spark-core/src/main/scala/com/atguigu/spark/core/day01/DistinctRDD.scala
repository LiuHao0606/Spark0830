package com.atguigu.spark.core.day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object DistinctRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MapRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(10, 60, 70, 50, 65, 35, 25, 12,60))
    val rdd2: RDD[Int] = rdd1.distinct()
    rdd2.collect.foreach(println)
    sc.stop()
  }
}
