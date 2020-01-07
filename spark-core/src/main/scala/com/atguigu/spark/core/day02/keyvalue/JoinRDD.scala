package com.atguigu.spark.core.day02.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object JoinRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("JoinRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    var rdd1 = sc.parallelize(Array((1, "a"), (1, "b"), (2, "c"),(6,"f")))
    var rdd2 = sc.parallelize(Array((1, "aa"), (3, "bb"), (2, "cc")))
    //val rdd3: RDD[(Int, (String, String))] = rdd1.join(rdd2)
    //val rdd3: RDD[(Int, (String, Option[String]))] = rdd1.leftOuterJoin(rdd2)
   //val rdd3: RDD[(Int, (Option[String], String))] = rdd1.rightOuterJoin(rdd2)

    val rdd3: RDD[(Int, (Option[String], Option[String]))] = rdd1.fullOuterJoin(rdd2)

    rdd3.collect.foreach(println)

  }
}
