package com.atguigu.day01.udf

import org.apache.spark.sql.{DataFrame, SparkSession}

object MySumUDFTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("MySumUDFTest")
      .getOrCreate()

    spark.udf.register("mySum",new MySumUDF)

    val df: DataFrame = spark.read.json("H:\\Test\\1.txt")
    df.createTempView("user")
    val dfSQL: DataFrame = spark.sql("select mySum(age) sumAge from user")
    dfSQL.show()

    spark.stop()


  }
}
