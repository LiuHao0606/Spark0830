package com.atguigu.spark.core.MyTest02

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}


object MyPartitioner {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MyPartitioner").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("warn")
    val rdd1: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8,50,40,30,20,10))
    val rdd2: RDD[(Int, String)] = rdd1.map((_, "hello"))
    val rdd3: RDD[(Int, String)] = rdd2.partitionBy(new MyPartitioner(2))
    rdd3.glom().collect().foreach(it=>println(it.mkString(",")))
    sc.stop()
  }
}
class MyPartitioner(val num:Int) extends Partitioner{
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    val i: Int = key.asInstanceOf[Int]
    (i%2).abs
  }

  override def hashCode(): Int = super.hashCode()

  override def equals(obj: Any): Boolean = super.equals(obj)
}
