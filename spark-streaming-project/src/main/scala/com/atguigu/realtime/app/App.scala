package com.atguigu.realtime.app

import com.atguigu.realtime.bean.AdsInfo
import com.atguigu.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait App {
  def main(args: Array[String]): Unit = {
      //1.创建StreamingContext
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("App")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("realtime")

    //从kafka读取数据
    val sourceDStream: DStream[AdsInfo] = MyKafkaUtil.getKafkaStream(ssc, "ads_log").map(s => {
      val splits: Array[String] = s.split(",")
      AdsInfo(splits(0).toLong, splits(1), splits(2), splits(3), splits(4))
    })


    //2.操作DStream
    doSomething(sourceDStream)

    //3.启动ssc，阻止main方法退出
    ssc.start()
    ssc.awaitTermination()
  }

  def doSomething(sourceDStream: DStream[AdsInfo]):Unit
}
