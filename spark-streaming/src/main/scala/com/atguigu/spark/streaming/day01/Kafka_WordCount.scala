package com.atguigu.spark.streaming.day01



import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Kafka_WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Kafka_WordCount")
    val ssc = new StreamingContext(conf, Seconds(3))

    val params: Map[String, String] = Map[String, String](
      ConsumerConfig.GROUP_ID_CONFIG->"0830",
      "bootstrap.servers"->"hadoop102:9092,hadoop103:9092,hadoop104:9092"
    )
    val sourceDstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, params, Set("s0830"))
    sourceDstream.map(_._2).
      flatMap(_.split("\\W+")).
      map((_,1)).reduceByKey(_+_).
      print(100)
    ssc.start()
    ssc.awaitTermination()
  }
}
