package com.atguigu.spark.core.MyTest01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDFlatMap {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDDFlatMap").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1: RDD[String] = sc.parallelize(Array("HELLO WORLD", "HELLO HELLO", "ATGUIGU ATGUIGU"))
    val rdd2: RDD[String] = rdd1.flatMap(a => a.split(" "))
    val rdd3: RDD[(Int, String)] = rdd2.mapPartitionsWithIndex((index, it) => it.map((index, _)))
    rdd3.collect().foreach(println)
    sc.stop()

  }
}
