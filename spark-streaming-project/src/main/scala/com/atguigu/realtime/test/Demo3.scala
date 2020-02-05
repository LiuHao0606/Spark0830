package com.atguigu.realtime.test

object Demo3 {
  def main(args: Array[String]): Unit = {
//    def aa()={
//      println("123")
//      100
//    }
//    foo(aa)
  }

  def foo(a: =>Int)={
      println(a)
      println(a)
  }

  class A{
    val a: Int =10
    var b: Int =20
    def c: Int =30
  }

  class B extends A{
    override val a: Int = 100//val只能重写val
    override val c: Int = 200

    //override var b: Int = 300 //无法重写，var只能重写抽象var
  }
}
