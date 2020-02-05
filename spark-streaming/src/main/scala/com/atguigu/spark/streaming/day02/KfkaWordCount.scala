package com.atguigu.spark.streaming.day02

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KfkaWordCount {

  def createSSC()={
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("./ck1")

    val params: Map[String, String] = Map[String, String](
      ConsumerConfig.GROUP_ID_CONFIG->"0830",
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG->"hadoop102:9092,hadoop103:9092,hadoop104:9092"
    )
    val sourceDStream: InputDStream[(String, String)] = KafkaUtils.
      createDirectStream[String, String, StringDecoder, StringDecoder](ssc, params, Set("s0830"))
    sourceDStream.map(_._2).
      flatMap(_.split("\\W+")).
      map((_,1)).reduceByKey(_+_).
      print(100)
    ssc
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck1", createSSC)

    ssc.start
    ssc.awaitTermination()
  }
}
