package com.atguigu.test02.udf

import org.apache.spark.sql.{DataFrame, SparkSession}

object MySumTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MySumTest")
      .master("local[*]")
      .getOrCreate()
    spark.udf.register("mysum",new MySum)
    val df: DataFrame = spark.read.json("H:\\Test\\1.txt")
    df.createTempView("user")
    spark.sql("select * from user").show()
    spark.sql("select mysum(age) from user").show()

    spark.close()

  }
}
