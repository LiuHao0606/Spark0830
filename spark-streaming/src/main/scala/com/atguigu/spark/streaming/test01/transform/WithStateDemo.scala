package com.atguigu.spark.streaming.test01.transform

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WithStateDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().
      setMaster("local[2]").
      setAppName("WithStateDemo  ")
    val ssc = new StreamingContext(conf, Seconds(3))
    //ssc.checkpoint("./ck2")
    ssc.sparkContext.setCheckpointDir("./ck3")
    val sourceDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    val resultDStream: DStream[(String, Int)] = sourceDStream
      .flatMap(_.split(" "))
      .map((_, 1))
      .updateStateByKey[Int]((seq:Seq[Int], opt:Option[Int]) => {
      Some(seq.sum + opt.getOrElse(0))
    })

    resultDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
