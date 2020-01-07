package com.atguigu.spark.core.day02.keyvalue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object FolfByKeyRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("FolfByKeyRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[(String, Int)] = sc.parallelize(Array(("a", 20), ("c", 10), ("d", 15), ("e", 16),
      ("b", 22), ("d", 66), ("d", 11), ("a", 20)))

    //foldByKey:AggregateByKey的简化版，适用于分区内与分区间数据一致
    val rdd2: RDD[(String, Int)] = rdd1.foldByKey(0)(_ + _)

    rdd2.collect().foreach(println)
    sc.stop()
  }
}
