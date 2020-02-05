package com.atguigu.realtime.test

object Demo6 {
  def main(args: Array[String]): Unit = {
    val A:Int=20
    val a:Int=20

    //打印10
    10 match {
      case a=>println(a)//如果是小写字母，表示重新定义变量，与上面的a不一样
      case _=>println("无法匹配")
    }

    //打印无法匹配
    10 match {
      case A=>println(A)//如果是大写字母，表示一个已经定义的变量，与上面得A一致2
      case _=>println("无法匹配")
    }
  }
}
