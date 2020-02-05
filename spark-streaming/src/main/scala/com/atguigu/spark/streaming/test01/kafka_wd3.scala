package com.atguigu.spark.streaming.test01

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object kafka_wd3 {

  val params = Map(
    ConsumerConfig.GROUP_ID_CONFIG -> "0830",
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092")
  private val kafkaCluster = new KafkaCluster(params)

  //读取Offsets
  def readOffsets():Map[TopicAndPartition,Long]={
    var resultMap: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()

    val topicAndPartitionEither: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(Set("s0830"))
//    if(topicAndPartitionEither.isRight){}
    topicAndPartitionEither match {
        //分区存在，就获取分区和偏移量
      case Right(topicAndPartitionSet)=>
        val topicAndPartitionOffsetEither: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluster.getConsumerOffsets("0830", topicAndPartitionSet)
        if(topicAndPartitionOffsetEither.isRight){//表示曾经消费过，有offset
          val topicAndPartitionOffsetMap: Map[TopicAndPartition, Long] = topicAndPartitionOffsetEither.right.get
          resultMap ++= topicAndPartitionOffsetMap
        }else{ //表示第一次消费分区，把每个分区的偏移量重置为0
          topicAndPartitionSet.foreach(tap=>{
            resultMap += tap -> 0L
          })
        }
      case _=>
    }
    resultMap
  }

  //提交Offsets
  def writeOffsets(sourceDstream: InputDStream[String]) = {

    sourceDstream.foreachRDD(rdd=>{
      val hasOffsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
      val ranges: Array[OffsetRange] = hasOffsetRanges.offsetRanges
      var map: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()
      ranges.foreach(range=>{
        val offset: Long = range.untilOffset
        map += (range.topicAndPartition() -> offset)
      })
      kafkaCluster.setConsumerOffsets("0830",map)
    })
  }

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("kafka_wd3").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))


    val sourceDstream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
      ssc,
      params,
      readOffsets(),
      (mm:MessageAndMetadata[String,String])  => mm.message()
    )
    sourceDstream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print(100)
    writeOffsets(sourceDstream)

    ssc.start()
    ssc.awaitTermination()

  }
}
