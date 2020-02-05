package com.atguigu.test02.hive

import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveSQL {
  def main(args: Array[String]): Unit = {


    System.setProperty("HADOOP_USER_NAME","atguigu")
    val spark: SparkSession = SparkSession.builder()
      .appName("HiveOprate")
      .master("local[2]")
      .enableHiveSupport() //支持HIVE
      .config("spark.sql.warehouse.dir","hdfs://hadoop102:9000/user/hive/warehouse")//创建数据库时位置指向hdfs,否则就存在本地
      .getOrCreate()
    import spark.implicits._

    //从外置hive读数据
//    spark.sql("use gmall")
//    spark.sql("select * from ads_uv_count").show()

    //从外置hive写数据
    val seq: Seq[(String, Int)] = Seq(("zhangsan", 20), ("lisi", 23))
    val df: DataFrame = seq.toDF("name", "age")
    //此方法插入数据时，表必须不存在
    //插入表中的数据需要与表列数据列名一致（忽略顺序）
    //df.write.saveAsTable("user0830")
    //此方法插入数据时（追加数据），表必须存在，
    //插入表中的数据需要与表列数据位置一致（忽略列名）
    df.write.insertInto("user0830")

    spark.close()
  }
}
