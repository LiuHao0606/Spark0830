package com.atguigu.day02

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object JDBCRead {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("JDBCRead")
      .master("local[2]")
      .getOrCreate()

    //通用读jdbc
//    val df: DataFrame = spark.read.format("jdbc")
//      .option("url", "jdbc:mysql://hadoop100:3306/test")
//      .option("user", "root")
//      .option("password", "123456")
//      .option("dbtable", "stu")
//      .load()
//    df.createTempView("stu")
//    spark.sql("select * from stu").show()

    //专用读jdbc
      val properties = new Properties()
      properties.setProperty("user","root")
      properties.setProperty("password","123456")
    val df: DataFrame = spark.read.jdbc("jdbc:mysql://hadoop100:3306/test", "stu", properties)
    df.createTempView("stu")
    spark.sql("select * from stu").show()

    spark.close()

  }
}
