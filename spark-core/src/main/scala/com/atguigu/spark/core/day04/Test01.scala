package com.atguigu.spark.core.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Test01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd2: RDD[Int] = sc.parallelize(Array(3, 5, 8, 2))
    var a=1
    val rdd3: RDD[(Int, Int)] = rdd2.map(x => {
      a += 1
      println(a)
      (x, 1)
    })

    rdd3.collect()
    println("----------------")
    println(a)
    sc.stop()

  }
}
