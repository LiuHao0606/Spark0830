package com.atguigu.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0831.common.util.ConstantUtil
import com.atguigu.realtime.bean.StartupLog
import com.atguigu.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp1 {
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
    //startupLogDStream .print()

    //3.去重
    //3.1从redis中读取启动记录,把启动的过滤
    val filterDStream: DStream[StartupLog] = startupLogDStream.transform(rdd => {
      val client: Jedis = RedisUtil.getJedisClient
      val midSet: util.Set[String] = client.smembers(ConstantUtil.STARTUP_TOPIC + ":" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
      val bd: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(midSet)
      client.close()
      rdd.filter(log =>
        !bd.value.contains(log.mid))
    })

    //再次去重：防止一个周期内（3秒内）一个设备启动两次（mid出现多次），并redis中不存在
    val distinctDStream: DStream[StartupLog] = filterDStream.map(log => (log.mid, log)).groupByKey.map {
      //case (mid, logIt) => logIt.toList.sortBy(_.ts).head
      case (mid, logIt) => logIt.toList.minBy(_.ts)//等价于上面的.sortBy(_.ts).head
    }

    //4.把第一次启动的设备写入redis
    distinctDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(it=>{
        val client: Jedis = RedisUtil.getJedisClient
        it.foreach(log=>{
          client.sadd(ConstantUtil.STARTUP_TOPIC + ":" + log.logDate,log.mid)
        })
        client.close()
      })
    })

    //5.写入Hbase
    import  org.apache.phoenix.spark._
    distinctDStream.foreachRDD(rdd=>{
      rdd.saveToPhoenix(
        "GMALL0830_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "TS", "LOGDATE", "LOGHOUR"),
        zkUrl = Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })



    //6.启动ssc
    ssc.start()
    ssc.awaitTermination()
  }
}
