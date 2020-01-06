package com.atguigu.spark.core.MyTest01

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDDKV {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDDKV").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1: RDD[(Int, Int)] = sc.parallelize(Array((1, 1), (2, 1), (3, 1),(4,1) ,(5, 1),(6,1)))
    val rdd2: RDD[(Int, Int)] = rdd1.partitionBy(new HashPartitioner(2))
    rdd2.glom().collect().foreach(arr=>println(arr.mkString(",")))
    sc.stop()
  }
}
