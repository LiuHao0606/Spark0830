package com.atguigu.spark.core.MyTest01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDDSample {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDDSample").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8,50,40,30,20,10))
    //sample：抽样，参数一：是否放回(如果放回可能重复抽到)，参数二：抽象的比例
    val rdd2: RDD[Int] = rdd1.sample(false, 0.5)
    rdd2.collect().foreach(println)
    sc.stop()
  }
}
