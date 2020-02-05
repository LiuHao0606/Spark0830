package com.atguigu.spark.core.MyTest04

import org.apache.spark.util.AccumulatorV2

class MyLongAcc extends AccumulatorV2[Long,Long]{

  private var sum:Long=0

  //判断零值
  override def isZero: Boolean = sum==0

  //复制累加器
  override def copy(): AccumulatorV2[Long, Long] = {
    val acc = new MyLongAcc
    acc.sum=this.sum
    acc
  }

  //重置累加器：复制到excutor的累加器需要重新开始计数
  override def reset(): Unit = sum=0

  //核心方法，累加：分区内累加
  override def add(v: Long): Unit = {
    sum+=v
  }

  //核心方法：合并：每个分区数据合并
  override def merge(other: AccumulatorV2[Long, Long]): Unit = {
     other match {
       case o:MyLongAcc=>sum+=o.sum
       case _=>throw new IllegalArgumentException("非法参数")
     }
  }

  //返回值
  override def value: Long = sum
}
