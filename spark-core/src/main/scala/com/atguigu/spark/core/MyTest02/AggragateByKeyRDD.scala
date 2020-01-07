package com.atguigu.spark.core.MyTest02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggragateByKeyRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("AggragateByKeyRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[(String, Int)] = sc.parallelize(List(("a", 2), ("c", 5), ("a", 6),("b",7),("d",3),
      ("b", 6), ("d", 7), ("d", 8),("a",9),("c",8)), 2)

    //aggregateByKey:参数1.初始值 参数2.分区内聚合 参数3.分区间聚合
    //val rdd2: RDD[(String, Int)] = rdd1.aggregateByKey(0)(_ + _, _ + _)
    //println(rdd2.collect().mkString(","))

    //1.求rdd1的k每个分区最大值，并将每个分区的最大值累加
    //val rdd3: RDD[(String, Int)] = rdd1.aggregateByKey(Int.MinValue)((m, v) => m.max(v), _ + _)

    //2.求rdd1的k的最大值，最小值======>(k,max,min)
//    val rdd3: RDD[(String, (Int, Int))] = rdd1.aggregateByKey((Int.MinValue, Int.MaxValue))((mm, v) => {
//      (mm._1.max(v), mm._2.min(v))
//    }, (mm1, mm2) => {
//      (mm1._1.max(mm2._1), mm1._2.min(mm2._2))
//    })

    //3.求rdd1的k的平均值
    val rddSum: RDD[(String, (Int, Int))] = rdd1.aggregateByKey((0, 0))((sumCount, v) => {
      (sumCount._1 + v, sumCount._2 + 1)
    }, (sc1, sc2) => {
      (sc1._1 + sc2._1, sc1._2 + sc2._2)
    })
    rddSum.collect().foreach(println)
    //val rdd3: RDD[(String, Double)] = rddSum.map(t => (t._1, t._2._1 / t._2._2.toDouble))
    val rdd3: RDD[(String, Double)] = rddSum.mapValues(t => (t._1 / t._2.toDouble))

    rdd3.collect().foreach(println)

    sc.stop()
  }
}
//(k,max,min)
//(k,avg)