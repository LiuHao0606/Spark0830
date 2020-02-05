package com.atguigu.realtime.util



import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

object MyKafkaUtil {
    def getKafkaStream(ssc:StreamingContext,topic:String)={

      val params = Map(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertiesUtil.getProperty("config.properties", "kafka.broker.list"),
        ConsumerConfig.GROUP_ID_CONFIG -> PropertiesUtil.getProperty("config.properties", "kafka.group")
      )

      KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
        ssc,
        params,
        Set(topic)
      )
    }
}
