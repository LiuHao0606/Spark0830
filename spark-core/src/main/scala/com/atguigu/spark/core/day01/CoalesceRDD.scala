package com.atguigu.spark.core.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CoalesceRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CoaleaseRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(10, 60, 70, 50, 65, 35, 25, 12),3)
    //coalesce:减少分区,默认没有shuffle，一般只能减少分区
    val rdd2: RDD[Int] = rdd1.coalesce(2)
    //coalesce:增加分区,shuffle设置为true才能增加分区成功
    val rdd3: RDD[Int] = rdd1.coalesce(4,true)

    //repartition：重置分区，一定会发生shuffle，底层调用coalesce(num,true)
    val rdd4: RDD[Int] = rdd1.repartition(4)


    //判断是否shuffle：一个分区的数据是否进入了多个分区，如果一个分区数据只进入了一个分区，则没有shuffle
    //增加分区必须shuffle，减少分区可以选择是否shuffle

    sc.stop()

  }
}
