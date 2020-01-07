package com.atguigu.spark.core.MyTest02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CombineByKeyRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("CombineByKeyRDD")
    val sc = new SparkContext(conf)
    val rdd1: RDD[String] = sc.parallelize(Array("hello", "hello", "world", "hello", "atguigu", "atguigu", "spark", "spark"))
    val rdd2: RDD[(String, Int)] = rdd1.map(s => (s, 1))

    //combineByKey: 参数1:计算零值,参数2:分区内聚合,参数3:分区间聚合
    val rdd3: RDD[(String, Int)] = rdd2.combineByKey(v => v,
      (last: Int, v: Int) => last + v, (v1: Int, v2: Int) => v1 + v2)

    rdd3.collect().foreach(println)


  }
}
