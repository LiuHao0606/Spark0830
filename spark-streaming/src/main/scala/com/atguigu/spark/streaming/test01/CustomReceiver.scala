package com.atguigu.spark.streaming.test01

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

object CustomReceiver{
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CustomReceiver").setMaster("local[2]")
    val sc = new StreamingContext(conf, Seconds(3))

    val rdstream: ReceiverInputDStream[String] = sc.receiverStream(new CustomReceiver("hadoop102", 9999))
    val dstream: DStream[(String, Int)] = rdstream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    dstream.print(100)

    sc.start()
    sc.awaitTermination()

  }
}

class CustomReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){

  //接收器启动时候调用的方法
  override def onStart(): Unit = {
       new Thread(){
         override def run(): Unit = receiveData()
       }.start()
  }

  //接收器停止的时候回调方法
  override def onStop(): Unit = ???

  def receiveData(): Unit = {
    try {
      val socket: Socket = new Socket(host, port)
      val stream: InputStream = socket.getInputStream
      val reader = new InputStreamReader(stream, "UTF-8")
      val buffer = new BufferedReader(reader)

      var line: String = buffer.readLine()
      while (line != null) {
        store(line)
        line = buffer.readLine()
      }
      buffer.close()
      socket.close()
    } catch {
      case e:Exception =>e.printStackTrace()
    }finally {
      restart("重启任务")
    }

  }
}
