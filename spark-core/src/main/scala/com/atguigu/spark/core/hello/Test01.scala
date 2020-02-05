package com.atguigu.spark.core.hello

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Test01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CoaleaseRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(10, 60, 70, 50, 80, 35, 90, 12),4)
    val sum: Int = rdd1.aggregate(100)(_.max(_), _ + _)
    println(sum)
    sc.stop()
  }
}
