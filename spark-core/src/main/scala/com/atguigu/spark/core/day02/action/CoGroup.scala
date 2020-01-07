package com.atguigu.spark.core.day02.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CoGroup {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CoGroup").setMaster("local[2]")
    val sc = new SparkContext(conf)
    var rdd1 = sc.parallelize(Array((1, "a"), (1, "b"), (2, "c"),(6,"f")))
    var rdd2 = sc.parallelize(Array((1, "aa"), (3, "bb"), (2, "cc")))

    val rdd3: RDD[(Int, (Iterable[String], Iterable[String]))] = rdd1.cogroup(rdd2)

    rdd3.collect.foreach(println)

    sc.stop()
  }
}
