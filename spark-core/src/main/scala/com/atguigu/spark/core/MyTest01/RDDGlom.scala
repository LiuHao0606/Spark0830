package com.atguigu.spark.core.MyTest01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDDGlom {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDDGlom").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8))
    //glom:将相同分区组成一个数组
    val rdd2: RDD[Array[Int]] = rdd1.glom()
    println(rdd2)
    rdd2.collect().foreach(x=>println(x.mkString(",")))

    val array: Array[Array[Int]] = rdd2.collect()
    println(array.toList)

    sc.stop()
  }
}
