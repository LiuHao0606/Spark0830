package com.atguigu.spark.core.MyTest04

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object BroadCastTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("addTest3").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(3, 5, 8, 2, 7, 9,9,8),2)

    val set = Set(8, 9)
    val bc: Broadcast[Set[Int]] = sc.broadcast(set)
    val rdd2: RDD[Int] = rdd1.filter(s => bc.value.contains(s))

    rdd2.collect().foreach(println)

    //广播变量：解决大变量读的问题

    sc.stop()
  }
}
