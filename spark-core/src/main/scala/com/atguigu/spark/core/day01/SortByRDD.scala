package com.atguigu.spark.core.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

object SortByRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val arr: Array[String] = Array("aaa", "abc", "dce", "case", "ascw", "dsggf", "rfdh", "popl")
    val rdd1: RDD[String] = sc.parallelize(arr)
    //val rdd2: RDD[Int] = rdd1.sortBy(x => x,false)

    //rdd2.collect().foreach(println)
    //按照字符串长度进行升序，长度相同，按照字典顺序进行降序
    val rdd2: RDD[String] = rdd1.sortBy(s => (s.length, s))(Ordering.Tuple2(Ordering.Int, Ordering.String), ClassTag(classOf[(Int, String)]))
    rdd2.collect.foreach(println)
    sc.stop()

  }
}


//带名参数,协变逆变