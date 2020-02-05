package com.atguigu.spark.core.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object MyPartition {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("JobTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("./ck1")
    var rdd1 = sc.parallelize(Array(10,20,30))
    val rdd2: RDD[(Int, Int)] = rdd1.map(x => (x, 1))
    val rdd3: RDD[(Int, Int)] = rdd2.partitionBy(new MyPartition(2))
    val rddl: RDD[((Int, Int), Long)] = rdd3.zipWithIndex()
    rddl.collect().foreach(println)
    sc.stop()
  }
}

class MyPartition(val parNum:Int) extends Partitioner{
  override def numPartitions: Int = parNum

  override def getPartition(key: Any): Int = key match {
    case null=>0
    case _=>key.hashCode().abs%numPartitions
  }

  override def hashCode(): Int = parNum
  override def equals(obj: Any): Boolean =obj match {
    //case null=>false
    case p:MyPartition=>p.parNum==parNum
    case _=>false
  }
}
