package com.atguigu.spark.core.MyTest04

import org.apache.spark.util.AccumulatorV2

class MyMapAcc extends AccumulatorV2[Long,Map[String,Double]] {

  private var map = Map[String, Double]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[Long, Map[String, Double]] = {
    val mapAcc = new MyMapAcc
    mapAcc.map=map
    mapAcc
  }

  override def reset(): Unit = map= Map[String, Double]()

  override def add(v: Long): Unit = {
    map += "sum"->(map.getOrElse("sum",0D)+v)
    map += "count"->(map.getOrElse("count",0D)+1)
  }

  override def merge(other: AccumulatorV2[Long, Map[String, Double]]): Unit = {
    other match {
      case o:MyMapAcc=>
        map += "sum"->(map.getOrElse("sum",0D)+o.map.getOrElse("sum",0D))
        map += "count"->(map.getOrElse("count",0D)+o.map.getOrElse("count",0D))
      case _=>throw new UnsupportedOperationException
    }
  }

  override def value: Map[String, Double] = {
    map += "avg"->(map.getOrElse("sum",0D) / map.getOrElse("count",0D))
    map
  }
}
