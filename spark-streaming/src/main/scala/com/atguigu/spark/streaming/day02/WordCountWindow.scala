package com.atguigu.spark.streaming.day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountWindow {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCountWindow")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("ck5")
    ssc.socketTextStream("hadoop102",9999)
      .flatMap(_.split("\\W+"))
      .map((_,1))
      //.reduceByKeyAndWindow(_+_,Seconds(15))
      .reduceByKeyAndWindow(_+_,_-_,Seconds(15))
        .print(1000)
    ssc.start()
    ssc.awaitTermination()

  }
}
