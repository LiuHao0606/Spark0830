package com.atguigu.spark.core.day02.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object AggregateByKeyRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("AggregateByKeyRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[(String, Int)] = sc.parallelize(Array(("a", 20), ("c", 10), ("d", 15), ("e", 16),
      ("b", 22), ("d", 66), ("d", 11), ("a", 20)))

    //aggregateByKey：预聚合与最后聚合逻辑可以不一致
    //val rdd2: RDD[(String, Int)] = rdd1.aggregateByKey(0)(_ + _, _ + _)

    //求出每个分区最大值的，最后求出最大值的和
    //val rdd2: RDD[(String, Int)] = rdd1.aggregateByKey(Int.MinValue)((x, y) => x.max(y), (x, y) => x + y)

    //计算出每个分区的最大值，最小值的和 a->(3,2)
//    val rdd2: RDD[(String, (Int, Int))] = rdd1.aggregateByKey((Int.MinValue, Int.MaxValue))(
//      { case ((max, min), v) => (max.max(v), min.min(v)) },
//      { case ((max1, min1), (max2, min2)) => (max1 + max2, min1 + min2) }
//    )

    //求出每个key的平均值
    val rdd2: RDD[(String, Double)] = rdd1.aggregateByKey((0, 0))(
      { case ((sum, count), v) => (sum + v, count + 1) },
      { case ((s1, c1), (s2, c2)) => (s1 + s2, c1 + c2) }
    ).map(t => (t._1, t._2._1 / t._2._2.toDouble))

    rdd2.collect().foreach(println)


  }
}
