package com.atguigu.realtime.test

object demo1 {
  def main(args: Array[String]): Unit = {
    val arr: Array[Int] = Array(30, 50, 70, 60, 10, 20)
    //arr.foreach(println)

    val myPrint: Any => Unit = println(_)
    arr.foreach(myPrint)
    myPrint(2)
  }
}
