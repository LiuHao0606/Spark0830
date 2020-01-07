package com.atguigu.spark.core.day02.action

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object PartitionBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("PartitionBy").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(10, 60, 70, 50,60),1)
    val rdd2: RDD[(Int, Int)] = rdd1.map((_, 1))
    val rdd3: RDD[(Int, Int)] = rdd2.partitionBy(new HashPartitioner(2))

    rdd3.glom().collect().foreach(it=>println("分区："+it.mkString(",")))
    sc.stop()
  }
}
