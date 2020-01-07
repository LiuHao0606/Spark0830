package com.atguigu.spark.core.day02.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CombineByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CombineByKey").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[(String, Int)] = sc.parallelize(Array(("a", 20), ("c", 10), ("d", 15), ("e", 16),
      ("b", 22), ("d", 66), ("d", 11), ("a", 16)))

    val rdd2: RDD[(String, Int)] = rdd1.combineByKey(x => x, (_: Int) + _, (_: Int) + _)
  //
    rdd2.collect().foreach(println)
  }
}
