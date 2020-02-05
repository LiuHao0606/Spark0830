package com.atguigu.spark.streaming.test01

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object kafka_wd1 {
  def main(args: Array[String]): Unit = {
    //1.创建StreamingContext
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("kafka_wd1")
    val ssc = new StreamingContext(conf, Seconds(3))

    val params = Map(
      ConsumerConfig.GROUP_ID_CONFIG -> "0830",
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    val sourceDStream: InputDStream[(String, String)] =
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, params, Set("s0830"))

    sourceDStream.map(_._2)
      .flatMap(_.split("\\W+"))
      .map((_,1))
      .reduceByKey(_+_).print(1000)

    ssc.start()
    ssc.awaitTermination()

  }
}
