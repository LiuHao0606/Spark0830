package com.atguigu.spark.core.day03

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object CheckPoint {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("JobTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("./ck1")
    var rdd1 = sc.parallelize(Array(10,20,30))
    val rdd2: RDD[(Int, Int)] = rdd1.map(s => {
      println(s+"====>")
      (s, 1)
    })
    val rdd3: RDD[(Int, Int)] = rdd2.filter(t => {
      println(t._1+"---->")
      true
    })

    rdd3.checkpoint()//checkpoint会单独启动一个job做运算，将数据缓存到磁盘
    rdd3.collect()
    println("------------------")
    rdd3.collect()
    rdd3.collect()

    Thread.sleep(10000000)
    sc.stop()
  }
}
