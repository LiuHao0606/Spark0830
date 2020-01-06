package com.atguigu.spark.core.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FilterRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("FilterRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(10, 60, 70, 50, 65, 35, 25, 12))
    //val rdd2: RDD[Int] = rdd1.filter(x => x > 50)

    //使用flatMap实现过滤
    //val rdd2: RDD[Int] = rdd1.flatMap(x => if (x > 50) Array(x) else Nil)

    //使用偏函数实现过滤
    val rdd2: RDD[Int] = rdd1.collect {
      case x: Int if x > 50 => x
    }


    println(rdd2.collect.mkString(","))



    sc.stop()
  }
}
