package com.atguigu.spark.core.day02.keyvalue

object TestMax {
  def main(args: Array[String]): Unit = {
    //println(max(1, 2)) //异常：Int没有实现Comparable
    println(max("a", "b"))//通过，String实现了Comparable

    println(max1(1, 2))
    println(max1("a", "b"))

    println(max2(1, 2))
    println(max2("a", "b"))
  }
  def max[T <: Comparable[T]](a:T,b:T):T={
    if(a.compareTo(b)>0) a else b
  }
  def max1[T](a:T,b:T)(implicit ord: Ordering[T]):T={
    ord.max(a,b)
  }

  /**
   * 泛型上下文，max1的简写
   * @param a
   * @param b
   * @tparam T
   * @return
   */
  def max2[T : Ordering](a:T,b:T):T={
    val ord: Ordering[T] = implicitly[Ordering[T]]
    ord.max(a,b)
  }

}

