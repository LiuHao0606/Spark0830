package com.atguigu.spark.core.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MapPartitionsRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(10, 60, 70, 50, 65, 35, 25, 12),3)
    //val rdd2: RDD[Double] = rdd1.mapPartitions(it => it.map(math.pow(_, 2)))
    val rdd3: RDD[(Int, Int)] = rdd1.mapPartitionsWithIndex((index, it) => it.map((index, _)))
    println(rdd3.collect.mkString(","))
    sc.stop()


    //切片（分区规则）
    /*
     (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      }

     */

  }
}
