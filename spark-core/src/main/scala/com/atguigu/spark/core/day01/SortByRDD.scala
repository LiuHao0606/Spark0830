package com.atguigu.spark.core.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortByRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //val rdd1: RDD[Int] = sc.parallelize(Array("aaa", "abc", "dce", "case", "ascw", "dsggf", "rfdh", "popl")
    //val rdd2: RDD[Int] = rdd1.sortBy(x => x,false)

    //rdd2.collect().foreach(println)

    sc.stop()

  }
}


//带名参数,协变逆变