package com.atguigu.spark.core.MyTest01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDAgrrateByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDDAgrrateByKey").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("warn")
    val rdd1: RDD[(String, Int)] = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)),2)
    //val rdd2: RDD[(String, Int)] = rdd1.aggregateByKey(Int.MinValue)((m, v) => m.max(v), _ + _)
   // val rdd2: RDD[(String, Int)] = rdd1.aggregateByKey(Int.MinValue)((m, v) => Math.max(m,v), _ + _)
   //val rdd2: RDD[(String, Int)] = rdd1.aggregateByKey(Int.MinValue)(Math.max(_,_), _ + _)
   val rdd2: RDD[(String, Int)] = rdd1.aggregateByKey(Int.MinValue)(Math.max, _ + _)

    rdd2.collect().foreach(println)

    sc.stop()
  }
}
