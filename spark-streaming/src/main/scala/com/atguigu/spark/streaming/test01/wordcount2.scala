package com.atguigu.spark.streaming.test01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object wordcount2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("wordcount2").setMaster("local[2]")
    val sc = new StreamingContext(conf, Seconds(3))

    val rddQueue: mutable.Queue[RDD[Int]] = mutable.Queue[RDD[Int]]()
    val ds: DStream[Int] = sc.queueStream(rddQueue, false).reduce(_ + _)
    ds.print()


    sc.start()
    while (true){
      rddQueue.enqueue(sc.sparkContext.parallelize(1 to 100))
      Thread.sleep(1000)
    }
    sc.awaitTermination()

  }
}
