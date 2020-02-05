package com.atguigu.realtime.app

import com.atguigu.realtime.bean.AdsInfo
import com.atguigu.realtime.util.MyRedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object AreaAdsClickTop3App extends App {
  override def doSomething(sourceDStream: DStream[AdsInfo]): Unit = {
    //AdsInfo(1579085082153,华中,杭州,101,5,2020-01-15 18:44:42.153,2020-01-15,18:44)
    //每天每地区热门广告 Top3  华北->{"2":1200,"9":110,"13":910}

    val dayAreaAndCount: DStream[((String, String, String), Int)] = sourceDStream.map(adsInfo => ((adsInfo.dayString, adsInfo.area, adsInfo.adsId), 1))
      .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => Some(seq.sum + opt.getOrElse(0)))
    val resultDstream: DStream[((String, String), List[(String, Int)])] = dayAreaAndCount.map {
      case ((day, area, adsId), count) => ((day, area), (adsId, count))
    }.groupByKey
      .map {
        case ((day, area), it) => {
          ((day, area), it.toList.sortBy(-_._2).take(3))
        }
      }
    resultDstream.foreachRDD(rdd=>{
      rdd.foreachPartition((it: Iterator[((String, String), List[(String, Int)])]) =>{
        val client: Jedis = MyRedisUtil.getJedisClient
        it.foreach{
          case ((day, area), list) =>
            import org.json4s.JsonDSL._
            val jsonStr: String = JsonMethods.compact(JsonMethods.render(list))
            client.hset(s"day:area:adsId:$day", area, jsonStr)
        }
        client.close()
      })
    })


  }
}
