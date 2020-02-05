package com.atguigu.spark.core.MyTest04

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}
/*
1.定义累加器
2.注册累加器
 */
object addTest2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("addTest2").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(3, 5, 8, 2, 7, 9))

    val acc: MyLongAcc = new MyLongAcc
    //使用累加器之前，先对累加器注册到
    sc.register(acc,"myacchahaaha")

    val rdd2: RDD[Int] = rdd1.map(x => {
      acc.add(1)
      x
    })

    rdd2.collect
    println(acc.value)
    Thread.sleep(1000000000)
    sc.stop()
  }
}
