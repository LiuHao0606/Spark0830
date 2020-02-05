package com.atguigu.realtime.test

object Demo7 {
  def main(args: Array[String]): Unit = {
    val arr: Array[Int] = Array(30, 50, 70, 60, 10, 20)
    val map1: Array[(Int, Int)] = arr.map((_, 1))
    map1.foreach{
      case(w,c)=>println(w)
    }
  }
}
