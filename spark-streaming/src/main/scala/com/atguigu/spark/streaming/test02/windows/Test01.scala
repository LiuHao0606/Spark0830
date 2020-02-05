package com.atguigu.spark.streaming.test02.windows

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Test01 {
  def main(args: Array[String]): Unit = {
    //1.创建StreamingContext
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Test01")
    val ssc = new StreamingContext(conf, Seconds(3))
    val sourceDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
   // sourceDStream.flatMap(_.split(" ")).map((_,1)).reduceByKeyAndWindow((_:Int)+(_:Int),Seconds(9)).print(100)
    sourceDStream.flatMap(_.split(" ")).map((_,1)).reduceByKeyAndWindow((_:Int)+(_:Int),Seconds(12),Seconds(6)).print(100)

    ssc.start()
    ssc.awaitTermination()
  }
}
