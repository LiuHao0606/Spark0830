package com.atguigu.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.logging.SimpleFormatter

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0831.common.util.ConstantUtil
import com.atguigu.realtime.bean.StartupLog
import com.atguigu.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DauApp").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))
    //1.从kafka获取启动日志数据
    val sourceDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, ConstantUtil.STARTUP_TOPIC)

    //2.封装数据
    val startupLogDStream = sourceDStream.map {
      case (_, value) =>
        val log = JSON.parseObject(value, classOf[StartupLog])
        log.logDate = new SimpleDateFormat("yyyy-MM-dd").format(log.ts)
        log.logHour = new SimpleDateFormat("HH").format(log.ts)
        log
    }
    startupLogDStream .print()

    //3.数据过滤,对写入过的做过滤，返回没有写过的
    val filterStartupLogDStream: DStream[StartupLog] = startupLogDStream.transform(rdd => {
      val client: Jedis = RedisUtil.getJedisClient
      val uidSet: util.Set[String] = client.smembers(ConstantUtil.REDIS_DAU_KEY + ":" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
      val uidSetBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(uidSet)
      client.close()
      rdd.filter(startupLog=>{
        val uids: util.Set[String] = uidSetBC.value
        !uids.contains(startupLog.uid)
      })
    })
    //4.去重
    val filterStartupLogDStream1: DStream[StartupLog] = filterStartupLogDStream
      .map(log => (log.uid, log))
      .groupByKey
      .map {
        case (_, logIt) => logIt.toList.sortBy(_.ts).head
      }
    //5.写入到redis
    filterStartupLogDStream1.foreachRDD(rdd=>{
      rdd.foreachPartition(it=>{
        val client: Jedis = RedisUtil.getJedisClient
        it.foreach(startupLog=>{
          client.sadd(ConstantUtil.STARTUP_TOPIC+":"+startupLog.logDate,startupLog.uid)
        })
        client.close()
      })
    })


    //6.写入Hbase
    import  org.apache.phoenix.spark._
    filterStartupLogDStream1.foreachRDD(rdd=>{
        rdd.saveToPhoenix(
          "GMALL_DAU",
          Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "TS", "LOGDATE", "LOGHOUR"),
          zkUrl = Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })

    //6.启动ssc
    ssc.start()
    ssc.awaitTermination()
  }
}
