package com.atguigu.test01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object CreateDS {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("RDD2DF")
      .master("local[2]")
      .getOrCreate()
    //此处spark对应上面的spark变量名
    import spark.implicits._

    val seq = Seq(User("zhangsan", 10), User("lisi", 20))
    val ds: Dataset[User] = seq.toDS()
    ds.createTempView("user")


    val df: DataFrame = spark.sql("select *from user")
    df.show()


    spark.close()

  }
}
