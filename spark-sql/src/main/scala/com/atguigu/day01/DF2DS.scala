package com.atguigu.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DF2DS {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("DF2DS")
      .getOrCreate()
    import spark.implicits._
    val rdd1: RDD[User] = spark.sparkContext.parallelize(Array(User("zhangsan", 20, "male"), User("lisi", 18, "female")))
    val df: DataFrame = rdd1.toDF
    val ds: Dataset[User] = df.as[User]
    ds.show

    ds.toDF().show

  }
}
