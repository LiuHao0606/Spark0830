package com.atguigu.spark.streaming.test01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object wordcount1 {
  def main(args: Array[String]): Unit = {
      //1.创建StreamingContext
      val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("wordcount1")
      val ssc = new StreamingContext(conf, Seconds(4))//4秒处理一次

     //2.核心数据集：DStreaming
     val socketStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //3.操作DStreaming
    val wcDStream: DStream[(String, Int)] = socketStream.flatMap(_.split(" "))
      .map((_, 1)).reduceByKey(_ + _)

    //4.最终数据的处理，打印
    wcDStream.print(100)

    //5.启动 teamingContext
    ssc.start()

    //6.阻止当前线程退出
    ssc.awaitTermination()

  }
}
