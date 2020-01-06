package com.atguigu.spark.core.hello

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Hello {
  def main(args: Array[String]): Unit = {
    //1.创建 SparkContext 如果打包放置在linux上的spark-submit提交需要删除.setMaster("local[2]")
    //val conf: SparkConf = new SparkConf().setAppName("单词统计").setMaster("local[2]")
    val conf: SparkConf = new SparkConf().setAppName("单词统计")

    val sc: SparkContext = new SparkContext(conf)

    //2.创建RDD
    //val rdd1: RDD[String] = sc.textFile("H:\\Test\\mrinput\\keyvalue")
    val rdd1: RDD[String] = sc.textFile("hdfs://hadoop102:9000/input")

    //3.对RDD进行操作
    val wordcount: RDD[(String, Int)] =
      rdd1.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    //4.执行一个行动算子
    val result: Array[(String, Int)] = wordcount.collect

    result.foreach(println)

    //5.关闭 SparkContext
    sc.stop()


  }
}
