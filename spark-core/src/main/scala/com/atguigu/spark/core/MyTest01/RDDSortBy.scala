package com.atguigu.spark.core.MyTest01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.spark_project.jetty.server.Authentication.User

object RDDSortBy {
  implicit val ord: Ordering[User]=new Ordering[User] {
    override def compare(x: User,y:User): Int ={
      println("123")
      x.id-y.id
    }
  }
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDDSortBy").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
   //val rdd1: RDD[Int] = sc.parallelize(Array(10,30,60,90,50,15,70,90,88),2)
    //val rdd2: RDD[Int] = rdd1.sortBy(i => i)
//    val ints: Array[Int] = rdd2.collect()
//    for(i <- ints){
//      println(i.toString.trim)
//    }
    val rdd1: RDD[User] = sc.parallelize(Array(User(1, "a"),User(6, "c"),User(5, "e"),
      User(8, "g"),User(2, "d")))
    val rdd2: RDD[User] = rdd1.sortBy(u => u, false)


    rdd2.collect().foreach(println)

    sc.stop()
  }
}

case class User(id:Int,name:String)
