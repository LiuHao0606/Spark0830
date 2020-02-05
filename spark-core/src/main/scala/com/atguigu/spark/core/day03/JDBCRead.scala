package com.atguigu.spark.core.day03

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object JDBCRead {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().
      setAppName("JDBCRead").setMaster("local[2]")
    val sc = new SparkContext(conf)


    val rdd1: JdbcRDD[(Int, String)] = new JdbcRDD[(Int, String)](
      sc,
      () => {
        Class.forName("com.mysql.jdbc.Driver")
        DriverManager.getConnection("jdbc:mysql://hadoop100:3306/test103","root","123456")
      },
      "select * from user where id >= ? and id <= ?",
      1,
      3,
      2,
      rs => {
        (rs.getInt(1), rs.getString(2))
      }
    )

    rdd1.collect().foreach(println)

    sc.stop()
  }
}
