package com.atguigu.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object RDD2DF_2 {
  def main(args: Array[String]): Unit = {
    //1.初始化SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("RDD2DF")
      .getOrCreate()
    import spark.implicits._

    val rdd1: RDD[(String, Int, String)] = spark .sparkContext.parallelize(Array(("zhangsan", 20, "male"), ("lisi", 18, "female")))
    val rowRDD: RDD[Row] = rdd1.map {
      case (name, age, sex) => Row(name, age, sex)
    }
    val st: StructType = StructType(Array(StructField("name", StringType), StructField("age", IntegerType), StructField("sex", StringType)))

   val df: DataFrame = spark.createDataFrame(rowRDD, st)


    df.show
    spark.close()
  }
}
