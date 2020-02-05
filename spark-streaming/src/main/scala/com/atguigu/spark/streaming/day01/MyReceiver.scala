package com.atguigu.spark.streaming.day01

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver



/**
 * 接收Socket数据
 */
class MyReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){

  var socket:Socket= _
  var reader:BufferedReader=_
  //当接收器启动时候回调这个函数
  override def onStart(): Unit = {

    runThread{
      try {
        socket = new Socket(host, port)
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream, "utf-8"))
        var line: String = reader.readLine()
        while (socket.isConnected && line != null) {
          store(line)
          line = reader.readLine()
        }
      } catch {
        case e:Exception =>e.printStackTrace()
      }finally {
        restart("重启receiver......")
      }

    }
  }

  //当接收器停止时候调用这个函数：释放资源
  override def onStop(): Unit = {
    if(socket!=null){
      socket.close()
    }
    if (reader!=null){
      reader.close()
    }
  }

  def runThread(f: =>Unit): Unit ={
    new Thread(){
      override def run(): Unit = f
    }.start()
  }
}

object MyReceiverTest{
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("MyReceiverTest")
    val ssc = new StreamingContext(conf, Seconds(3))
    val lineStream: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("hadoop102", 9999))
    lineStream.flatMap(_.split("\\W+")).map((_,1)).reduceByKey(_+_).print(100)
    ssc.start()
    ssc.awaitTermination()
  }
}