package com.atguigu.spark.core.day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GroupByRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("FilterRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[(String, Int)] = sc.parallelize(Array(("a", 20), ("c", 10), ("d", 15),
      ("e", 16), ("b", 22), ("d", 66), ("d", 11), ("a", 20)))
    val rdd2: RDD[(String, Iterable[(String, Int)])] = rdd1.groupBy(t => t._1)
    rdd2.collect().foreach(println)

  }
}
