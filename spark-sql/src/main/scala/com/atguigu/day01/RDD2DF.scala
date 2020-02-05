package com.atguigu.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object RDD2DF {
  def main(args: Array[String]): Unit = {
    //1.初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("RDD2DF")
      .getOrCreate()
    import spark.implicits._
    //2.创建rdd
    //val rdd1: RDD[String] = spark.sparkContext.parallelize(Array("hello", "hello", "world", "hello", "atguigu"))
    //val df = rdd1.toDF("word")

//    val rdd1: RDD[(String, Int, String)] = spark.sparkContext.parallelize(Array(("zhangsan", 20, "nan"), ("lisi", 118, "nv")))
//    val df: DataFrame = rdd1.toDF("name", "age", "gender")

    val rdd1: RDD[User] = spark.sparkContext.parallelize(Array(User("zhangsan", 20, "male"), User("lisi", 18, "female")))
    val df: DataFrame = rdd1.toDF

    df.show(1000)

    spark.close()
  }
}
case class User(name:String,age:Int,gender:String)