package com.atguigu.spark.core.day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SampleRDD {


  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MapRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(10, 60, 70, 50, 65, 35, 25, 12))
    //withReplacement：是否放回；  fraction：抽象比例
    val rdd2: RDD[Int] = rdd1.sample(false, 0.5)
    rdd2.collect().foreach(println)


    sc.stop()
  }
}
