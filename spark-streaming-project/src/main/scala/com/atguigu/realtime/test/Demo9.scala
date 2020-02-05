package com.atguigu.realtime.test

object Demo9 {
  def main(args: Array[String]): Unit = {
//    val bb: Array[BB] = Array[BB](new BB)
//    val aa: Array[AA]=bb

    val bbs: List[BB] = List[BB](new BB)
    val aas: List[AA] = bbs

  }
}

class AA
class BB extends AA
