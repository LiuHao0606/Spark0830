package com.atguigu.spark.core.day03

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object JDBCWrite {
  def main(args: Array[String]): Unit = {
      val conf: SparkConf = new SparkConf().setAppName("JDBCWrite").setMaster("local[2]")
      val sc = new SparkContext(conf)
      val rdd1: RDD[(Int, String)] = sc.parallelize(Array((6, "mayun"), (7, "mahuateng"),
        (8, "liyahhong")))

      Class.forName("com.mysql.jdbc.Driver")
      rdd1.foreachPartition(it=> {
        val conn: Connection = DriverManager.getConnection("jdbc:mysql://hadoop100:3306/test103",
          "root", "123456")
        it.foreach {
          case (id, name) =>
            val ps: PreparedStatement = conn.prepareStatement("insert into user values(?,?)")
            ps.setInt(1, id)
            ps.setString(2, name)
            ps.execute()
            ps.close()
        }
        conn.close()
      }
      )
      rdd1.collect
      println("执行完成！")
      sc.stop

 }
}
