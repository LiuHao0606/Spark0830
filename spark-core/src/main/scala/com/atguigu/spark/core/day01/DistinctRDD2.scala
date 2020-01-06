package com.atguigu.spark.core.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object DistinctRDD2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DistinctRDD2").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Product] = sc.parallelize(Array(User(1, "zhangsan"), User(2, "lisi"),
      User(3, "wangwu"),User(1,"hello")))

    val rdd2: RDD[Product] = rdd1.distinct(2)
    rdd2.collect.foreach(println)
    sc.stop()
  }
}

case class User(id :Int,name :String)
