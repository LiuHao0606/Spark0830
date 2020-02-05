package com.atguigu.spark.streaming.test01.transform

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().
      setMaster("local[2]").
      setAppName("TransformDemo")
    val ssc = new StreamingContext(conf, Seconds(3))
    val sourceDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    val resultDStream: DStream[(String, Int)] = sourceDStream.transform(rdd => {
      rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    })
    resultDStream.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
