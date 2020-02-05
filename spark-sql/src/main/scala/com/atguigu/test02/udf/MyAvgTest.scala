package com.atguigu.test02.udf

import org.apache.spark.sql.{DataFrame, SparkSession}

object MyAvgTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MyAvgTest")
      .master("local[*]")
      .getOrCreate()
    spark.udf.register("myavg",new MyAvg)
    val df: DataFrame = spark.read.json("H:\\Test\\1.txt")
    df.createTempView("user")
    spark.sql("select * from user").show()
    spark.sql("select myavg(age) from user").show()

    spark.close()

  }
}
