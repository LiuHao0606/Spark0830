package com.atguigu.spark.core.day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GlomRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("FilterRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(10, 60, 70, 50, 65, 35, 25, 12))
    //glom:把每个分区元素放入一个数组
    val rdd2: RDD[Array[Int]] = rdd1.glom()

    rdd2.collect.foreach(x=>println(x.mkString(",")))



    sc.stop()
  }
}
