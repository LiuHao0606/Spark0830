package com.atguigu.spark.core.day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object DoubleValue {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(10, 60, 70, 50, 65, 35, 25, 12,10))
    val rdd2: RDD[Int] = sc.parallelize(Array(60,50,30,20,10))
    val rdd3: RDD[Int] = rdd1.union(rdd2)
   // rdd3.collect().foreach(println)

    val rdd4: RDD[Int] = rdd1.subtract(rdd2)
    //rdd4.collect().foreach(println)

    val rdd5: RDD[Int] = rdd1.intersection(rdd2)
    //rdd5.collect().foreach(println)

    /*
      zip:分区数一致，每个分区个数一样
     */
    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    //rdd6.collect().foreach(println)

    //zipWithIndex：元素和自己的索引进行拉链
    val rdd7: RDD[(Int, Long)] = rdd1.zipWithIndex()
    //rdd7.collect().foreach(println)

    //zipPartitions:分区必须一样
    val rdd8: RDD[(Int, Int)] = rdd1.zipPartitions(rdd2)((it1, it2) => {
      //it1.zip(it2)
      it1.zipAll(it2,666,888)
    })
    rdd8.collect().foreach(println)


    sc.stop()
  }
}
