package com.atguigu.spark.streaming.test02.windows

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Test02 {
  def main(args: Array[String]): Unit = {
    //1.创建StreamingContext
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Test02")
    val ssc = new StreamingContext(conf, Seconds(3))
    val sourceDStream: DStream[String] = ssc.socketTextStream("hadoop102", 9999).window(Seconds(12), Seconds(6))
    sourceDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print(100)

    ssc.start()
    ssc.awaitTermination()
  }
}
