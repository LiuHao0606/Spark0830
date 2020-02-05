package com.atguigu.realtime.test

object MapDemo {
  def main(args: Array[String]): Unit = {
    val arr: Array[Int] = Array(30, 50, 70, 60, 10, 20)
    val arr1: Array[Int] = myMap(arr, _ + 1)
    println(arr1.mkString(","))
  }

  def myMap(arr:Array[Int],op: Int=>Int)={
    for(e <- arr) yield op(e)
  }
}
