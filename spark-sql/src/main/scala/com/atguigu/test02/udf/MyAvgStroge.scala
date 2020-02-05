package com.atguigu.test02.udf

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator



class MyAvgStroge extends Aggregator[User,SumCount,Double]{
  //缓冲区零值
  override def zero: SumCount = SumCount(0,0)

  override def reduce(b: SumCount, a: User): SumCount = {
     SumCount(b.sum+a.age,b.count+1)
  }

  override def merge(b1: SumCount, b2: SumCount): SumCount = {
    SumCount(b1.sum+b2.sum,b1.count+b2.count)
  }

  override def finish(reduction: SumCount): Double = reduction.sum.toDouble / reduction.count

  override def bufferEncoder: Encoder[SumCount] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

case class User(name:String,age:Long)
case class SumCount(sum: Long,count:Long)

object MyAvgStrogeTest{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MyAvgStrogeTest")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.read.json("H:\\Test\\1.txt")
    val ds: Dataset[User] = df.as[User]

    //注册自定义方法
    val myAvgStroge = new MyAvgStroge
    val column: TypedColumn[User, Double] = myAvgStroge.toColumn.name("myavg")
    ds.select(column).show()

     //df.select(column).show()



    spark.stop()
  }
}
