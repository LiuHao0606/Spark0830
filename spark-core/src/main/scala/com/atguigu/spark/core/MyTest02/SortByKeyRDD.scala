package com.atguigu.spark.core.MyTest02

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SortByKeyRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SortByKeyRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    var rdd1 = sc.parallelize(Array((1, "a"), (1, "b"), (2, "c"),(6,"g")))
    var rdd2 = sc.parallelize(Array((1, "aa"), (3, "bb"), (2, "cc")))
    //val rdd3: RDD[(Int, (String, String))] = rdd1.join(rdd2)
    //val rdd3: RDD[(Int, (String, Option[String]))] = rdd1.leftOuterJoin(rdd2)

    val rdd3: RDD[(Int, (Option[String], Option[String]))] = rdd1.fullOuterJoin(rdd2)

    rdd3.collect().foreach(println)

    sc.stop()
  }
}
