package com.atguigu.spark.streaming.day01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //1.创建StreamingContext
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val ssc = new StreamingContext(conf, Seconds(3))
    //2.从数据源得到DStream
    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    //3.对流进行转换
    val result: DStream[(String, Int)] = dstream
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
    //4.展示算子
    result.print(1000)
    //5.开启实时处理
    ssc.start()
    //6.阻止main线程退出
    ssc.awaitTermination()
  }
}
