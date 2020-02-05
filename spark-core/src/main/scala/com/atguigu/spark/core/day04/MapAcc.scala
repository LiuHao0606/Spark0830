package com.atguigu.spark.core.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

class MapAcc extends AccumulatorV2[Long,Map[String,Double]] {

  //用于返回最大值，最小值，总和，平均值的键值对
  private var map: Map[String, Double] = Map[String, Double]()
  //统计每个数字出现次数
  private var count:Double=0D

  override def isZero: Boolean = map.isEmpty&&count==0

  override def copy(): AccumulatorV2[Long, Map[String, Double]] = {
    val mapAcc = new MapAcc
    mapAcc.map=map
    mapAcc.count=count
    mapAcc
  }

  override def reset(): Unit = {
    map.empty
    count=0D
  }

  override def add(v: Long): Unit = {

    map+=("max"->map.getOrElse("max",Double.MinValue).max(v))
    map+=("min"->map.getOrElse("min",Double.MaxValue).min(v))
    map+=("sum"->(map.getOrElse("sum",0D)+v.toDouble))
    //每出现一个数字，count就加+1
    count+=1
  }

  override def merge(other: AccumulatorV2[Long, Map[String, Double]]): Unit = {
    other match {
      case o:MapAcc=>{
        map+=("max"-> (map.getOrElse("max",Double.MinValue).max(o.value.getOrElse("max",Double.MinValue))))
        map+=("min"-> (map.getOrElse("min",Double.MaxValue).min(o.value.getOrElse("min",Double.MaxValue))))
        map+=("sum"-> (map.getOrElse("sum",0D)+o.value.getOrElse("sum",0D)))
        count+=o.count
      }
      case _=>throw new IllegalArgumentException
    }
  }

  override def value: Map[String, Double] = {
    map += ("avg"->(map.getOrElse("sum",0D)/count))
    map
  }
}

object MapAccTest{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MapAccTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd2: RDD[Int] = sc.parallelize(Array(60,70,50,20,100))

    //创建之定义累加器
    val acc: MapAcc = new MapAcc
    //向sc注册自定义累加器
    sc.register(acc)

    rdd2.foreach(x=>acc.add(x))
    println(acc.value)
    sc.stop()

  }
}
