package com.atguigu.spark.streaming.day02

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KfkaWordCount2 {

  private val topics = Set("s0830")
  val groupId="0830"
  val params: Map[String, String] = Map[String, String](
    ConsumerConfig.GROUP_ID_CONFIG->groupId,
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG->"hadoop102:9092,hadoop103:9092,hadoop104:9092"
  )
  private val cluster = new KafkaCluster(params)

  def getOffsets(): Map[TopicAndPartition, Long] = {
    var result: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()
    val topicAndPartitionEither: Either[Err, Set[TopicAndPartition]] = cluster.getPartitions(topics)
    topicAndPartitionEither match {
      case Right(b) =>
        val topAndPartitions: Set[TopicAndPartition] =b
        val topicAndPartitionMapEither: Either[Err, Map[TopicAndPartition, Long]] =
          cluster.getConsumerOffsets(groupId, topAndPartitions)
        topicAndPartitionMapEither match {
          case Right(b1) =>
            val topAndPartitionMap: Map[TopicAndPartition, Long] = b1
            result ++= topAndPartitionMap
          case Left(a1) =>
            b.foreach(tpAndPartition=>{
              result += tpAndPartition -> 0L
            })
        }
      case Left(a) =>
    }
    result
  }

  def writeOffsets(sourceDStream: InputDStream[String]) = {
    sourceDStream.foreachRDD(rdd=>{
      val hasOffsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
      val offsetRanges: Array[OffsetRange] = hasOffsetRanges.offsetRanges
      var map: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()
      offsetRanges.foreach(range=>{
        val key: TopicAndPartition = range.topicAndPartition()
        val value: Long = range.untilOffset
        map += key ->value
      })
      cluster.setConsumerOffsets(groupId,map)
    })
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val ssc = new StreamingContext(conf, Seconds(3))

    val sourceDStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
      ssc, params, getOffsets(), (handle: MessageAndMetadata[String, String]) => handle.message()
    )

    writeOffsets(sourceDStream)

    sourceDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print(100)

    ssc.start
    ssc.awaitTermination()
  }
}
