package com.atguigu.spark.core.MyTest02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggragateByKeyRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("AggragateByKeyRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd2: RDD[(String, Int)] = sc.parallelize(List(("a", 2), ("c", 5), ("a", 6),("b",7),("b", 6), ("d", 7), ("d", 8),("a",6)), 2)
    //aggregateByKey:参数1.初始值 参数2.分区内聚合 参数3.分区间聚合
    val rdd3: RDD[(String, Int)] = rdd2.aggregateByKey(Int.MinValue)((m, v) => m.max(v), _ + _)



    rdd3.collect().foreach(println)

    sc.stop()
  }
}
