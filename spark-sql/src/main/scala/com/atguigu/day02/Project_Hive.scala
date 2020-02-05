package com.atguigu.day02

import org.apache.spark.sql.{DataFrame, SparkSession}

object Project_Hive {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("HiveOprate")
      .master("local[2]")
      .enableHiveSupport() //支持HIVE
      .config("spark.sql.warehouse.dir","hdfs://hadoop102:9000/user/hive/warehouse")
      .getOrCreate()
    import spark.implicits._
    spark.sql("use sql_project")

    spark.udf.register("cru",new CityRemarkUDAF1)

    val df: DataFrame = spark.sql(
      """select c.*,p.product_id,p.product_name from user_visit_action u join product_info p
        |on u.click_product_id=p.product_id
        |join city_info c
        |on u.city_id=c.city_id""".stripMargin)
    df.createTempView("t1")

    val df2: DataFrame = spark.sql(
      """select area,product_name,count(*) clickcount,cru(city_name) cn from t1
        |group by area,product_name""".stripMargin)
    df2.createTempView("t2")

    val df3: DataFrame = spark.sql(
      """select area,product_name,
        |       clickcount,
        |       rank() over (partition by area order by clickcount desc) rk,
        |       cn
        |from t2""".stripMargin)
    df3.createTempView("t3")

    spark.sql("select area,product_name,clickcount,cn from t3 where rk<=3").show(100)

    spark.close()
  }
}
