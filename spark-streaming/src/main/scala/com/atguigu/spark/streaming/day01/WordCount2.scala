package com.atguigu.spark.streaming.day01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object WordCount2 {
  def main(args: Array[String]): Unit = {
    //1.创建StreamingContext
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val ssc = new StreamingContext(conf, Seconds(3))
    //2.从数据源得到DStream
    val queue: mutable.Queue[RDD[Int]] = mutable.Queue[RDD[Int]]()
    val rddDstream: InputDStream[Int] = ssc.queueStream(queue,false)
    //3.对流进行转换
    val result: DStream[Int] = rddDstream.reduce(_ + _)
    //4.展示算子
    result.print(1000)
    //5.开启实时处理
    ssc.start()
    while(true){
      val rdd: RDD[Int] = ssc.sparkContext.parallelize(1 to 100)
      queue.enqueue(rdd)//queue += rdd
      Thread.sleep(2000)
    }
    //6.阻止main线程退出
    ssc.awaitTermination()
  }
}
