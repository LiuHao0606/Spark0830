package com.atguigu.spark.core.day02.keyvalue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SortByKeyRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CombineByKey").setMaster("local[2]")
    val sc = new SparkContext(conf)
//    val rdd1: RDD[(String, Int)] = sc.parallelize(Array(("a", 20), ("c", 10), ("d", 15), ("e", 16),
//      ("b", 22), ("d", 66), ("d", 11), ("a", 16)))

    implicit val ord:Ordering[User]=new Ordering[User] {
      override def compare(x: User, y: User): Int = x.id-y.id
    }

    val rdd1: RDD[User] = sc.parallelize(User(6, "c") :: User(9, "d") :: User(3, "a") :: User(1, "g") ::
      User(10, "f") :: User(5, "k") :: User(8, "l") :: Nil)
    val rdd2: RDD[(User, Int)] = rdd1.map((_, 1))
    val rdd3: RDD[(User, Int)] = rdd2.sortByKey()

    rdd3.collect().foreach(println)
  }
}

case class User(id:Int,name:String)