package com.atguigu.test01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DF2RDD {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("DF2RDD")
      .master("local[2]")
      .getOrCreate()

    val df: DataFrame = spark.read.json("H:\\Test\\1.txt")
    val rdd: RDD[Row] = df.rdd
    val rdd1: RDD[User] = rdd.map(row => {
      User(row.getString(1), row.getLong(0).toInt) //此处读取json顺序与row的顺序会不一致
    })
    rdd1.collect().foreach(println)

  }
}
