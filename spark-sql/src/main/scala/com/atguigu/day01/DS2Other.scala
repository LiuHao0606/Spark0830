package com.atguigu.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object DS2Other {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("RDD2DF")
      .getOrCreate()
    import spark.implicits._
    val rdd1: RDD[User] = spark.sparkContext.parallelize(Array(User("zhangsan", 20, "male"), User("lisi", 18, "female")))
    val ds: Dataset[User] = rdd1.toDS()

    val rdd2: RDD[User] = ds.rdd

  }
}
