package com.atguigu.test02.project

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SqlApp {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")
    val spark: SparkSession = SparkSession.builder()
      .appName("SqlApp")
      .master("local[*]")
      .enableHiveSupport() //支持HIVE
      .config("spark.sql.warehouse.dir","hdfs://hadoop102:9000/user/hive/warehouse")
      .getOrCreate()
    import spark.implicits._
    spark.sql("use sql_project")

    import spark.implicits._
    //注册聚合函数
    spark.udf.register("remark",RemarkUDAF)

    val df: DataFrame = spark.sql(
      """select c.*,p.product_id,p.product_name from user_visit_action u join product_info p
        |on u.click_product_id=p.product_id
        |join city_info c
        |on u.city_id=c.city_id""".stripMargin)
    df.createTempView("t1")

    val df2: DataFrame = spark.sql(
      """select area,product_name,
        |count(*) clickcount,
        |remark(city_name) remarks
        |from t1
        |group by area,product_name""".stripMargin)
    df2.createTempView("t2")

    val df3: DataFrame = spark.sql(
      """select area,product_name,
        |       clickcount,
        |       remarks,
        |       rank() over (partition by area order by clickcount desc) rk
        |from t2""".stripMargin)
    df3.createTempView("t3")

    val dfResult: DataFrame = spark.sql("select area,product_name,clickcount,remarks from t3 where rk<=3")
    //dfResult.show(100,false)
    dfResult.coalesce(1).write.mode(SaveMode.Overwrite).saveAsTable("cityCount_Rate")


    spark.close()
  }
}
