package com.atguigu.day02

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object JDBCWrite {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("JDBCWrite")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    //通用写
//    val rdd: RDD[User] = spark.sparkContext.parallelize(List(User(1001, "zhangsan", 9000.5), User(1002, "lisi", 25000.50)))
//    val df: DataFrame = rdd.toDF()
//    df.write.format("jdbc")
//          .option("url", "jdbc:mysql://hadoop100:3306/test")
//          .option("user", "root")
//          .option("password", "123456")
//          .option("dbtable", "person1")
//          .mode(SaveMode.Append)
//          .save()

    //专用读写
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123456")
    val rdd: RDD[User] = spark.sparkContext.parallelize(List(User(1001, "zhangsan", 9000.5), User(1002, "lisi", 25000.50)))
    val df: DataFrame = rdd.toDF()
    df.write.jdbc("jdbc:mysql://hadoop100:3306/test","person2",properties)


    spark.close()
  }
}

case class User(id:Int,name:String,salary:Double)