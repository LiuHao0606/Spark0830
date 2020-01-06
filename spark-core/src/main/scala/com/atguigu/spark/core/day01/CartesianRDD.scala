package com.atguigu.spark.core.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CartesianRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CartesianRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(1,2,3))
    val rdd2: RDD[Int] = sc.parallelize(Array(7,8,9))
    val rdd3: RDD[(Int, Int)] = rdd1.cartesian(rdd2)

    rdd3.collect().foreach(println)

    sc.stop()

  }
}
