package com.atguigu.spark.core.day03

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SaveText {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("JobTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("./ck1")
    var rdd1 = sc.parallelize(Array(10,20,30,40),2)
    rdd1.saveAsTextFile("G:\\0830")

    sc.stop()
  }
}
