package com.atguigu.spark.core.MyTest01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDDCoalease {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDDCoalease").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8,50,40,30,20,10),4)
    val rdd2: RDD[Int] = rdd1.coalesce(2)
    val partitions: Int = rdd2.getNumPartitions
    sc.stop()
  }
}
