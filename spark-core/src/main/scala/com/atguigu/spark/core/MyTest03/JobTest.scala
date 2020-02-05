package com.atguigu.spark.core.MyTest03

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object JobTest {
  def main(args: Array[String]): Unit = {
    /*
        application:表示一个应用，一个SparkContext表示一个app
        job：在一个应用中，每次调用一次行动算子，就会启动一个job
        stage：阶段，碰到一个需要shuffle的算子(宽依赖)，就会分一个阶段出来
        task：任务，一个阶段会包含多个task，由RDD的分区数（切片数）决定
     */


    val conf: SparkConf = new SparkConf().setAppName("JobTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    var rdd1 = sc.parallelize(Array(10,20,30))
    val rdd2: RDD[(Int, Int)] = rdd1.map(s => {
      println(s+"====>")
      (s, 1)
    })
    val rdd3: RDD[(Int, Int)] = rdd2.filter(t => {
      println(t._1+"---->")
      true
    })

    //rdd3.cache()//对rdd3做缓存，默认在内存汇中缓存

    rdd3.persist(StorageLevel.MEMORY_ONLY)//对rdd3做缓存，只在内存中做缓存
    rdd3.collect()
    println("------------------")
    rdd3.collect()
    rdd3.collect()

    Thread.sleep(10000000)
    sc.stop()
  }
}
