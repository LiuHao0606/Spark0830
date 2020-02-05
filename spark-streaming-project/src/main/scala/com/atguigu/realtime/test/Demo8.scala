package com.atguigu.realtime.test

import scala.reflect.ClassTag

object Demo8 {
  def main(args: Array[String]): Unit = {
    newArray[Int](10)
    newArray1[Int](10)
  }
  //ClassTag:运行时候记住泛型的类型
  def newArray[T:ClassTag](len:Int)={
    new Array[T](len)
  }

  def newArray1[T](len:Int)(implicit ct:ClassTag[T])={
    new Array[T](len)
  }
}
