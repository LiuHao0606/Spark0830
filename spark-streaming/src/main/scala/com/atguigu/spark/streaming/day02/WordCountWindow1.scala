package com.atguigu.spark.streaming.day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountWindow1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCountWindow")
    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.socketTextStream("hadoop102",9999).window(Seconds(15))
      .flatMap(_.split("\\W+"))
      .map((_,1))
      .reduceByKey(_+_)
      .print(1000)
    ssc.start()
    ssc.awaitTermination()

  }
}
