package com.atguigu.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DSDeme {
  def main(args: Array[String]): Unit = {
    //1.初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("RDD2DF")
      .getOrCreate()
    import spark.implicits._

    val rdd1 = Seq(User("zhangsan", 20, "male"), User("lisi", 18, "female"))
    val ds: Dataset[User] = rdd1.toDS
    ds.select("name").show()
    ds.filter(_.age>19).show
    ds.filter($"age">19).show

    spark.close()

  }
}
