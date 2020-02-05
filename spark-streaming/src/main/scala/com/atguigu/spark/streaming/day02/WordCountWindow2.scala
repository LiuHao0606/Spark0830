package com.atguigu.spark.streaming.day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountWindow2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCountWindow")
    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.socketTextStream("hadoop102",9999)
      .flatMap(_.split("\\W+"))
      .map((_,1))
      .reduceByKey(_+_)
      .saveAsTextFiles("log")

    ssc.start()
    ssc.awaitTermination()

  }
}
