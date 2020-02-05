package com.atguigu.spark.core.day03

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object HBaseRead {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("HBaseRead").setMaster("local[2]")
    val sc = new SparkContext(conf)


    sc.stop()
  }
}
