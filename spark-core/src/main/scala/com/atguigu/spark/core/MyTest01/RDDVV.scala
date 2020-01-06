package com.atguigu.spark.core.MyTest01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDDVV {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDDVV").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(10,30,60,90,50,15,70,90, 88,666,777))
    val rdd2: RDD[Int] = sc.parallelize(Array(1, 6, 9, 7, 8, 6, 5, 100,999))

    //val rdd3: RDD[Int] = rdd1.union(rdd2)
    //val rdd3: RDD[Int] = rdd1.subtract(rdd2)

    //val rdd3: RDD[(Int, Int)] = rdd1.zip(rdd2)
    val rdd3: RDD[(Int, Int)] = rdd1.zipPartitions(rdd2)((it1, it2) => {
      it1.zip(it2)
    })

    rdd3.glom().collect().foreach(arr=>println(arr.mkString(",")))

    sc.stop()
  }
}
