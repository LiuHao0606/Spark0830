package com.atguigu.test01

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object RDD2DF {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("RDD2DF")
      .master("local[2]")
      .getOrCreate()

    //此处spark对应上面的spark变量名
    import spark.implicits._
    val sc: SparkContext = spark.sparkContext
    val rdd1: RDD[(String, Int)] = sc.parallelize(List(("zhangsan", 10), ("lisi", 20)))
    val df: DataFrame = rdd1.toDF("name","age")
    df.show()

    val rdd2: RDD[User] = sc.parallelize(List(User("zhangsan", 20), User("lisi", 21)))
    val df2: DataFrame = rdd2.toDF()
    df2.show()

    println("df3==========================================")
    val rdd3: RDD[Row] = sc.parallelize(List(("zhangsan", 10), ("lisi", 20))).map {
      case (name, age) => Row(name, age)
    }
    val df3: DataFrame = spark.createDataFrame(rdd3,
      StructType(List(StructField("name", StringType), StructField("age", IntegerType))))
    df3.show()

    spark.stop()


  }
}

case class User(name:String,age:Int)