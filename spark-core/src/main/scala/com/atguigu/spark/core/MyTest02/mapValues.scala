package com.atguigu.spark.core.MyTest02

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object mapValues {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("AggragateByKeyRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[(String, Int)] = sc.parallelize(List(("a", 2), ("c", 5), ("a", 6),("b",7),("d",3),
      ("b", 6), ("d", 7), ("d", 8),("a",9),("c",8)), 2)


    val rdd2: RDD[(String, Int)] = rdd1.groupBy(t => t._1).mapValues(t => t.map(s => s._2).sum)
    rdd2.collect().foreach(println)

    sc.stop()
  }
}
