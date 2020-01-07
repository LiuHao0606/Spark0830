package com.atguigu.spark.core.day02.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Foreach {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Foreach").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(10, 60, 70, 50,60))
    //rdd1.foreach(println)
   rdd1.foreachPartition(it=>{
     it.foreach(println)
   })
    sc.stop()
  }
}
