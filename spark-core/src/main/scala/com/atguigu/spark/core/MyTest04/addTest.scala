package com.atguigu.spark.core.MyTest04

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

object addTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("addTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(3, 5, 8, 2,7))

    val acc: LongAccumulator = sc.longAccumulator("first")
    //var a:Int=0
    val rdd2: RDD[Int] = rdd1.map(x => {
      acc.add(1)
      x
    })

    rdd2.collect
    println(acc.value)

    sc.stop()
  }
}
