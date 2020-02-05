package com.atguigu.spark.core.day04

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object BroadcastTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BroadcastTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd2: RDD[Int] = sc.parallelize(Array(60,70,50,20,100))

    //广播变量
    val bd: Broadcast[Set[Int]] = sc.broadcast(Set(10, 20))

    rdd2.foreach(x=>{
      val set=bd.value
      println(set.contains(x))
    })

    sc.stop()

  }
}
