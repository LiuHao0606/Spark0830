package com.atguigu.realtime.app

import com.atguigu.realtime.bean.AdsInfo
import com.atguigu.realtime.util.MyRedisUtil
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object LastHourAdsClickApp extends App {
  override def doSomething(sourceDStream: DStream[AdsInfo]): Unit = {

    //计各广告最近 1 小时内的点击量趋势：各广告最近 1 小时内各分钟的点击量
    val adsAndcountDStream: DStream[(String, List[(String, Int)])] = sourceDStream.window(Minutes(60))
      .map(adsInfo => ((adsInfo.adsId, adsInfo.hmString), 1))
      .reduceByKey(_ + _)
      .map {
        case ((adsId, hm), count) => (adsId, (hm, count))
      }.groupByKey()
      .mapValues(it => it.toList.sortBy(_._1))

    adsAndcountDStream.foreachRDD(rdd => {
      println(System.currentTimeMillis()+":rdd循环:"+rdd)
      rdd.foreachPartition(it => {
        val list: List[(String, List[(String, Int)])] = it.toList
        if (list.size > 0) {
          val client: Jedis = MyRedisUtil.getJedisClient
          import org.json4s.JsonDSL._
          // 迭代器内所有的数据, 一次性的写入到redis
          val map: Map[String, String] = list.toMap.map {
            case (ads, it1) => (ads, JsonMethods.compact(JsonMethods.render(it1)))
          }

          import scala.collection.JavaConversions._
          client.hmset("last:hour:ads:click", map)
        }


      })
    })
  }
}
