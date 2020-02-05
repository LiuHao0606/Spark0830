package com.atguigu.spark.core.MyTest03

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CheckPoint {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CheckPoint").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("./ck1")

    val rdd1: RDD[String] = sc.parallelize(Array("hello"),4)
    val rdd2: RDD[(String, Int)] = rdd1.map(x => {
      println(System.currentTimeMillis())
      (x, 1)
    })
    rdd2.checkpoint()
    rdd2.collect
    println("======================")
    rdd2.collect

    sc.stop()
  }
}
