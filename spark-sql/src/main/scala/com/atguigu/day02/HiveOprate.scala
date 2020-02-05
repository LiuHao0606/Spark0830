package com.atguigu.day02

import org.apache.spark.sql.SparkSession

object HiveOprate {
  def main(args: Array[String]): Unit = {
    //如果运行过程中出现权限问题，可以加这行
    System.setProperty("HADOOP_USER_NAME","atguigu")
    val spark: SparkSession = SparkSession.builder()
      .appName("HiveOprate")
      .master("local[2]")
      .enableHiveSupport() //支持HIVE
      .config("spark.sql.warehouse.dir","hdfs://hadoop102:9000/user/hive/warehouse")//数据保存路径，默认为本地
      .getOrCreate()
    import spark.implicits._
    spark.sql("create database sql20200112")
    spark.sql("use sql20200112")
    spark.sql("show tables")
    spark.sql("create table user(id int,name String)")
    spark.sql("insert into user values(1,'zhangsan')")
    spark.sql("select * from  user").show()
    spark.stop()



  }
}
