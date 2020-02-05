package com.atguigu.spark.core.day04

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

object Test02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test02").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd2: RDD[Int] = sc.parallelize(Array(3, 5, 8, 2))

    val acc: LongAccumulator = sc.longAccumulator

    //var a=1
    val rdd3: RDD[(Int, Int)] = rdd2.map(x => {
      //a += 1
      //println(a)
      acc.add(1)
      (x, 1)
    })

    rdd3.collect()
    println("----------------")
    rdd3.collect()
    println(acc.value)//执行了两次collect()，且没有缓存，累加器又是同一个，则结果8
    sc.stop()
  }
}
