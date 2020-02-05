package com.atguigu.spark.core.day04

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

object MyAccTest{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MyAcc").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd2: RDD[Int] = sc.parallelize(Array(3, 5, 8, 2))

    //创建之定义累加器
    val acc: MyAcc = new MyAcc
    //向sc注册自定义累加器
    sc.register(acc)

    val rdd3: RDD[(Int, Int)] = rdd2.map(x => {
      acc.add(1)
      (x, 1)
    })

    rdd3.collect()
    println("----------------")
    rdd3.collect()
    println(acc.value)//执行了两次collect()，且没有缓存，累加器又是同一个，则结果8
    sc.stop()
  }
}

//自定义累加器
class MyAcc extends AccumulatorV2[Long,Long]{

  //缓存中间值
  var sum:Long=0L
  //累加器零值判断
  override def isZero: Boolean = sum==0
  //复制累加器
  override def copy(): AccumulatorV2[Long, Long] = {
    val newAcc = new MyAcc
    newAcc.sum=sum
    newAcc
  }
  //重置累加器,把缓存的值重置为零值
  override def reset(): Unit =sum=0
  //核心功能：累加
  override def add(v: Long): Unit = this.sum+=v

  //各个excutor之间进行合并
  override def merge(other: AccumulatorV2[Long, Long]): Unit = {
//    val o: MyAcc = other.asInstanceOf[MyAcc]
//    this.sum+=o.sum

    other match {
      case o:MyAcc=>this.sum+=o.sum
      case _=>throw new IllegalArgumentException
    }

  }
  //返回最终的累加值
  override def value: Long = sum
}
