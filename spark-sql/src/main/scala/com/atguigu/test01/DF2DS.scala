package com.atguigu.test01

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DF2DS {
  def main(args: Array[String]): Unit = {
     val spark: SparkSession = SparkSession.builder()
      .appName("DF2DS")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.read.json("H:\\Test\\1.txt")
    val ds: Dataset[User1] = df.as[User1]

    ds.show()
    spark.stop()
  }
}

case class User1(name:String,age:Long)