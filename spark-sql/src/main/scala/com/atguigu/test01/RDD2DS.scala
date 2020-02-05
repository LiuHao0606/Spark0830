package com.atguigu.test01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object RDD2DS {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("RDD2DS")
      .master("local[2]")
      .getOrCreate()
    //此处spark对应上面的spark变量名
    import spark.implicits._
    val rdd: RDD[User] = spark.sparkContext.parallelize(List(User("zhangsan", 10), User("lisi", 20)))
    val ds: Dataset[User] = rdd.toDS()

  }
}
